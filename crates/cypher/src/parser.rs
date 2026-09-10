use crate::{
    lexer::{self, Kind, PatternPunctuation, Token},
    syntax::*,
};
use helix_planner::relational::{self as r, QueryError, Result, Span};

pub fn parse(source: &str) -> Result<Statement> {
    Parser {
        source,
        tokens: lexer::lex(source)?,
        position: 0,
        depth: 0,
        pattern_mode: PatternMode::Build,
    }
    .statement()
}

struct Parser<'a> {
    source: &'a str,
    tokens: Vec<Token>,
    position: usize,
    depth: usize,
    pattern_mode: PatternMode,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PatternMode {
    Build,
    /// Recognition only: its temporary syntax is always discarded. Never nest
    /// probes, so ambiguous parentheses cannot cause exponential backtracking.
    Probe,
}

enum PatternContext {
    Expression,
    Comprehension,
}

impl Parser<'_> {
    fn token(&self) -> &Token {
        &self.tokens[self.position]
    }
    fn is(&self, s: &str) -> bool {
        match &self.token().kind {
            Kind::Word(word) => word.eq_ignore_ascii_case(s),
            Kind::Symbol(symbol) => *symbol == s,
            Kind::Escaped(_)
            | Kind::String(_)
            | Kind::Number(_)
            | Kind::Parameter(_)
            | Kind::Pattern(_)
            | Kind::End => false,
        }
    }
    fn take(&mut self, s: &str) -> bool {
        if self.is(s) {
            self.position += 1;
            true
        } else {
            false
        }
    }
    fn take_pattern(&mut self, punctuation: PatternPunctuation) -> bool {
        let ascii = match punctuation {
            PatternPunctuation::LeftArrow => "<",
            PatternPunctuation::RightArrow => ">",
            PatternPunctuation::Dash => "-",
        };
        if matches!(self.token().kind, Kind::Pattern(actual) if actual == punctuation) {
            self.position += 1;
            true
        } else {
            self.take(ascii)
        }
    }
    fn expect(&mut self, s: &str) -> Result<()> {
        if self.take(s) {
            Ok(())
        } else {
            Err(self.error(&format!("expected {s}")))
        }
    }
    fn error(&self, message: &str) -> QueryError {
        let detail = if matches!(self.token().kind, Kind::Pattern(_)) {
            "InvalidUnicodeCharacter"
        } else {
            "UnexpectedSyntax"
        };
        QueryError::compile("SyntaxError", detail, message).at(self.token().span)
    }
    fn name(&mut self) -> Result<String> {
        let name = match &self.token().kind {
            Kind::Word(s) | Kind::Escaped(s) => s.clone(),
            Kind::String(_)
            | Kind::Number(_)
            | Kind::Parameter(_)
            | Kind::Symbol(_)
            | Kind::Pattern(_)
            | Kind::End => return Err(self.error("expected identifier")),
        };
        self.position += 1;
        Ok(name)
    }
    fn optional_name(&mut self) -> Result<Option<String>> {
        if matches!(self.token().kind, Kind::Word(_) | Kind::Escaped(_)) {
            self.name().map(Some)
        } else {
            Ok(None)
        }
    }
    fn unsupported(&self, name: &str) -> QueryError {
        QueryError::unsupported(name).at(self.token().span)
    }

    fn statement(&mut self) -> Result<Statement> {
        let mut clauses = Vec::new();
        while !matches!(self.token().kind, Kind::End) && !self.is(";") {
            let clause = if self.take("OPTIONAL") {
                self.expect("MATCH")?;
                self.match_clause(true)?
            } else if self.take("MATCH") {
                self.match_clause(false)?
            } else if self.take("RETURN") {
                self.project(true)?
            } else if self.take("WITH") {
                self.project(false)?
            } else if self.take("UNWIND") {
                let expression = self.expr(0)?;
                self.expect("AS")?;
                Clause::Unwind {
                    expression,
                    name: self.name()?,
                }
            } else if self.take("CREATE") {
                Clause::Create(self.patterns()?)
            } else if self.take("SET") {
                let mut assignments = Vec::new();
                loop {
                    let target = self.expr(5)?;
                    if matches!(target.kind, ExprKind::HasLabel(..)) {
                        return Err(self.unsupported("LabelMutation"));
                    }
                    let add = self.take("+=");
                    if !add {
                        self.expect("=")?;
                    }
                    let value = self.expr(0)?;
                    let assignment = match target.kind {
                        ExprKind::Variable(name) => {
                            if add {
                                Assignment::Extend(name, value)
                            } else {
                                Assignment::Replace(name, value)
                            }
                        }
                        ExprKind::Property(..) if !add => Assignment::Property(target, value),
                        ExprKind::HasLabel(..) => return Err(self.unsupported("LabelMutation")),
                        ExprKind::Literal(_)
                        | ExprKind::Parameter(_)
                        | ExprKind::PatternPredicate
                        | ExprKind::Property(..)
                        | ExprKind::Index(..)
                        | ExprKind::Slice { .. }
                        | ExprKind::Unary(..)
                        | ExprKind::Binary(..)
                        | ExprKind::Call { .. }
                        | ExprKind::List(_)
                        | ExprKind::Map(_)
                        | ExprKind::Case { .. } => {
                            return Err(self.error("SET requires a property or entity"))
                        }
                    };
                    assignments.push(assignment);
                    if !self.take(",") {
                        break;
                    }
                }
                Clause::Set(assignments)
            } else if self.take("REMOVE") {
                Clause::Remove(self.expressions()?)
            } else if self.take("DETACH") {
                self.expect("DELETE")?;
                Clause::Delete {
                    expressions: self.expressions()?,
                    detach: true,
                }
            } else if self.take("DELETE") {
                Clause::Delete {
                    expressions: self.expressions()?,
                    detach: false,
                }
            } else {
                for (keyword, feature) in [
                    ("MERGE", "Merge"),
                    ("CALL", "ProceduresAndSubqueries"),
                    ("UNION", "Union"),
                    ("FOREACH", "Foreach"),
                    ("LOAD", "LoadCsv"),
                    ("DROP", "SchemaDdl"),
                ] {
                    if self.is(keyword) {
                        return Err(self.unsupported(feature));
                    }
                }
                return Err(self.error("expected a query clause"));
            };
            clauses.push(clause);
        }
        self.take(";");
        if !matches!(self.token().kind, Kind::End) {
            return Err(self.unsupported("MultipleStatements"));
        }
        if clauses.is_empty() {
            return Err(self.error("expected a statement"));
        }
        Ok(Statement { clauses })
    }

    fn expressions(&mut self) -> Result<Vec<Expr>> {
        let mut xs = vec![self.expr(0)?];
        while self.take(",") {
            xs.push(self.expr(0)?);
        }
        Ok(xs)
    }

    fn match_clause(&mut self, optional: bool) -> Result<Clause> {
        let patterns = self.patterns()?;
        let predicate = if self.take("WHERE") {
            Some(self.expr(0)?)
        } else {
            None
        };
        Ok(Clause::Match {
            patterns,
            optional,
            predicate,
        })
    }

    fn project(&mut self, returning: bool) -> Result<Clause> {
        let distinct = self.take("DISTINCT");
        let mut items = Vec::new();
        loop {
            if self.take("*") {
                items.push(Item::Wildcard);
            } else {
                let expression = self.expr(0)?;
                let text = self.source[expression.span.start..expression.span.end].to_owned();
                let alias = if self.take("AS") {
                    Some(self.name()?)
                } else {
                    None
                };
                items.push(Item::Expression {
                    expression,
                    alias,
                    text,
                });
            }
            if !self.take(",") {
                break;
            }
        }
        let mut ordering = Vec::new();
        if self.take("ORDER") {
            self.expect("BY")?;
            loop {
                let expression = self.expr(0)?;
                let descending = self.take("DESC") || self.take("DESCENDING");
                if !descending {
                    self.take("ASC");
                    self.take("ASCENDING");
                }
                ordering.push((expression, descending));
                if !self.take(",") {
                    break;
                }
            }
        }
        let skip = if self.take("SKIP") {
            Some(self.expr(0)?)
        } else {
            None
        };
        let limit = if self.take("LIMIT") {
            Some(self.expr(0)?)
        } else {
            None
        };
        let predicate = if !returning && self.take("WHERE") {
            Some(self.expr(0)?)
        } else {
            None
        };
        Ok(Clause::Project {
            returning,
            items,
            distinct,
            ordering,
            skip,
            limit,
            predicate,
        })
    }

    fn patterns(&mut self) -> Result<Vec<Pattern>> {
        let mut xs = vec![self.pattern()?];
        while self.take(",") {
            xs.push(self.pattern()?);
        }
        Ok(xs)
    }

    fn take_deferred_pattern(&mut self, context: PatternContext) -> Result<bool> {
        if self.pattern_mode == PatternMode::Probe {
            return Ok(false);
        }
        let (position, depth) = (self.position, self.depth);
        self.pattern_mode = PatternMode::Probe;
        let pattern = self.pattern();
        let end = self.position;
        self.position = position;
        self.depth = depth;
        self.pattern_mode = PatternMode::Build;
        let Ok(pattern) = pattern else {
            return Ok(false);
        };
        if pattern.relationships.is_empty() {
            return Ok(false);
        }
        let detail = match context {
            PatternContext::Expression => {
                self.position = end;
                return Ok(true);
            }
            PatternContext::Comprehension
                if matches!(&self.tokens[end].kind, Kind::Symbol("|"))
                    || matches!(&self.tokens[end].kind, Kind::Word(word) if word.eq_ignore_ascii_case("WHERE")) =>
            {
                "PatternComprehension"
            }
            PatternContext::Comprehension => return Ok(false),
        };
        Err(QueryError::unsupported(detail).at(Span {
            start: self.tokens[position].span.start,
            end: self.tokens[end - 1].span.end,
        }))
    }

    fn pattern(&mut self) -> Result<Pattern> {
        let name = if !self.is("(") {
            if self.is("shortestPath") || self.is("allShortestPaths") {
                return Err(self.unsupported("ShortestPath"));
            }
            let name = self.name()?;
            self.expect("=")?;
            Some(name)
        } else {
            None
        };
        let mut nodes = vec![self.node()?];
        let mut relationships = Vec::new();
        loop {
            // Keep arrowheads and dashes separate in the lexer: `<-` can also
            // be a scalar comparison followed by unary negation. Whitespace
            // and comments between pattern punctuation are legal in M23.
            let incoming = self.take_pattern(PatternPunctuation::LeftArrow);
            if !self.take_pattern(PatternPunctuation::Dash) {
                if incoming {
                    return Err(self.error("expected relationship dash after arrowhead"));
                }
                break;
            }
            let mut name = None;
            let mut types = Vec::new();
            let mut properties = Vec::new();
            if self.take("[") {
                name = self.optional_name()?;
                if self.take(":") {
                    types.push(self.name()?);
                    while self.take("|") {
                        self.take(":");
                        types.push(self.name()?);
                    }
                }
                if self.is("*") {
                    if self.pattern_mode == PatternMode::Build {
                        return Err(self.unsupported("VariableLengthPattern"));
                    }
                    // Probe enough of the pinned range grammar to distinguish
                    // a deferred pattern expression from scalar subtraction of
                    // a list. The resulting temporary pattern cannot be lowered.
                    self.position += 1;
                    if matches!(self.token().kind, Kind::Number(_)) {
                        self.position += 1;
                    }
                    if self.take("..") && matches!(self.token().kind, Kind::Number(_)) {
                        self.position += 1;
                    }
                }
                if matches!(self.token().kind, Kind::Parameter(_)) {
                    return Err(QueryError::compile(
                        "SyntaxError",
                        "InvalidParameterUse",
                        "relationship predicates in MATCH require a map literal",
                    )
                    .at(self.token().span));
                }
                if matches!(self.token().kind, Kind::Number(_)) || self.is("..") {
                    return Err(QueryError::compile(
                        "SyntaxError",
                        "InvalidRelationshipPattern",
                        "relationship bounds require an asterisk",
                    )
                    .at(self.token().span));
                }
                if self.is("{") {
                    properties = self.map()?;
                }
                self.expect("]")?;
            }
            if !self.take_pattern(PatternPunctuation::Dash) {
                return Err(self.error("expected relationship dash"));
            }
            let outgoing = self.take_pattern(PatternPunctuation::RightArrow);
            let direction = if incoming && outgoing {
                r::Direction::Undirected
            } else if incoming {
                r::Direction::Incoming
            } else if outgoing {
                r::Direction::Outgoing
            } else {
                r::Direction::Undirected
            };
            relationships.push(Relationship {
                name,
                types,
                direction,
                properties,
            });
            nodes.push(self.node()?);
        }
        Ok(Pattern {
            name,
            nodes,
            relationships,
        })
    }

    fn node(&mut self) -> Result<Node> {
        self.expect("(")?;
        let name = self.optional_name()?;
        let mut labels = Vec::new();
        while self.take(":") {
            labels.push(self.name()?);
        }
        let has_properties = self.is("{");
        let properties = if has_properties {
            self.map()?
        } else {
            Vec::new()
        };
        if matches!(self.token().kind, Kind::Parameter(_)) {
            return Err(self.unsupported("PatternParameterMap"));
        }
        self.expect(")")?;
        Ok(Node {
            has_properties,
            name,
            labels,
            properties,
        })
    }

    fn map(&mut self) -> Result<Vec<(String, Expr)>> {
        self.expect("{")?;
        let mut items = Vec::new();
        if !self.take("}") {
            loop {
                let name = self.name()?;
                self.expect(":")?;
                items.push((name, self.expr(0)?));
                if !self.take(",") {
                    break;
                }
            }
            self.expect("}")?;
        }
        Ok(items)
    }

    fn expr(&mut self, min: u8) -> Result<Expr> {
        self.depth += 1;
        if self.depth > r::MAX_EXPRESSION_DEPTH {
            return Err(QueryError::compile(
                "ResourceLimit",
                "ExpressionDepth",
                "expression nesting exceeds 48 levels",
            )
            .at(self.token().span));
        }
        let result = self.expr_inner(min);
        self.depth -= 1;
        result
    }

    fn expr_inner(&mut self, min: u8) -> Result<Expr> {
        let start = self.token().span.start;
        let mut left = if self.is("(") && self.take_deferred_pattern(PatternContext::Expression)? {
            ExprKind::PatternPredicate
        } else if self.take("NOT") {
            ExprKind::Unary(r::Unary::Not, Box::new(self.expr(4)?))
        } else if self.take("-") {
            if let Kind::Number(number) = &self.token().kind {
                let value = number_value(number, true, self.token().span)?;
                self.position += 1;
                ExprKind::Literal(value)
            } else {
                ExprKind::Unary(r::Unary::Negate, Box::new(self.expr(9)?))
            }
        } else if self.take("+") {
            ExprKind::Unary(r::Unary::Positive, Box::new(self.expr(9)?))
        } else if self.take("(") {
            let e = self.expr(0)?;
            self.expect(")")?;
            e.kind
        } else if self.take("[") {
            self.take_deferred_pattern(PatternContext::Comprehension)?;
            if matches!(&self.token().kind,Kind::Word(name) if !["true","false","null"].iter().any(|s|name.eq_ignore_ascii_case(s)))
                && matches!(self.tokens.get(self.position+1).map(|t|&t.kind),Some(Kind::Word(word)) if word.eq_ignore_ascii_case("IN"))
            {
                return Err(self.unsupported("ListComprehension"));
            }
            let mut xs = Vec::new();
            if !self.take("]") {
                xs = self.expressions()?;
                if self.is("|") {
                    return Err(self.unsupported("ListComprehension"));
                }
                self.expect("]")?;
            }
            ExprKind::List(xs)
        } else if self.is("{") {
            ExprKind::Map(self.map()?)
        } else if self.take("CASE") {
            let operand = if self.is("WHEN") {
                None
            } else {
                Some(Box::new(self.expr(0)?))
            };
            let mut branches = Vec::new();
            while self.take("WHEN") {
                let predicate = self.expr(0)?;
                self.expect("THEN")?;
                branches.push((predicate, self.expr(0)?));
            }
            if branches.is_empty() {
                return Err(self.error("CASE requires WHEN"));
            }
            let otherwise = if self.take("ELSE") {
                Some(Box::new(self.expr(0)?))
            } else {
                None
            };
            self.expect("END")?;
            ExprKind::Case {
                operand,
                branches,
                otherwise,
            }
        } else {
            let token = self.token().clone();
            self.position += 1;
            match token.kind {
                Kind::String(s) => ExprKind::Literal(r::Value::String(s)),
                Kind::Number(s) => ExprKind::Literal(number_value(&s, false, token.span)?),
                Kind::Parameter(name) => ExprKind::Parameter(name),
                Kind::Word(ref word) if word.eq_ignore_ascii_case("null") => {
                    ExprKind::Literal(r::Value::Null)
                }
                Kind::Word(ref word) if word.eq_ignore_ascii_case("true") => {
                    ExprKind::Literal(r::Value::Boolean(true))
                }
                Kind::Word(ref word) if word.eq_ignore_ascii_case("false") => {
                    ExprKind::Literal(r::Value::Boolean(false))
                }
                Kind::Word(name) | Kind::Escaped(name) => {
                    if self.take("(") {
                        if ["all", "any", "none", "single", "reduce"]
                            .iter()
                            .any(|n| name.eq_ignore_ascii_case(n))
                        {
                            return Err(self.unsupported("QuantifiedListExpression"));
                        }
                        if name.eq_ignore_ascii_case("shortestPath")
                            || name.eq_ignore_ascii_case("allShortestPaths")
                        {
                            return Err(self.unsupported("ShortestPath"));
                        }
                        let distinct = self.take("DISTINCT");
                        let star = self.take("*");
                        let arguments = if star || self.is(")") {
                            Vec::new()
                        } else {
                            self.expressions()?
                        };
                        self.expect(")")?;
                        ExprKind::Call {
                            name,
                            arguments,
                            distinct,
                            star,
                        }
                    } else {
                        ExprKind::Variable(name)
                    }
                }
                kind @ (Kind::Symbol(_) | Kind::Pattern(_) | Kind::End) => {
                    return Err(QueryError::compile(
                        "SyntaxError",
                        if matches!(kind, Kind::Pattern(_)) {
                            "InvalidUnicodeCharacter"
                        } else {
                            "UnexpectedSyntax"
                        },
                        "expected expression",
                    )
                    .at(token.span))
                }
            }
        };
        let mut expression = Expr::new(
            left,
            Span {
                start,
                end: self.tokens[self.position - 1].span.end,
            },
        )?;
        let mut comparison_tail: Option<Expr> = None;
        loop {
            if self.is(".") && min <= 10 {
                self.position += 1;
                let name = self.name()?;
                if self.is("(") {
                    return Err(self.unsupported("NamespacedFunction"));
                }
                left = ExprKind::Property(Box::new(expression), name);
            } else if self.is("[") && min <= 10 {
                self.position += 1;
                let start_index = if self.is("..") {
                    None
                } else {
                    Some(Box::new(self.expr(0)?))
                };
                if self.take("..") {
                    let end = if self.is("]") {
                        None
                    } else {
                        Some(Box::new(self.expr(0)?))
                    };
                    left = ExprKind::Slice {
                        value: Box::new(expression),
                        start: start_index,
                        end,
                    };
                } else {
                    left = ExprKind::Index(
                        Box::new(expression),
                        start_index.ok_or_else(|| self.error("expected index"))?,
                    );
                }
                self.expect("]")?;
            } else if self.is(":") && min <= 9 {
                self.position += 1;
                left = ExprKind::HasLabel(Box::new(expression), self.name()?);
            } else if self.is("IS") && min <= 5 {
                self.position += 1;
                let not = self.take("NOT");
                self.expect("NULL")?;
                left = ExprKind::Unary(
                    if not {
                        r::Unary::IsNotNull
                    } else {
                        r::Unary::IsNull
                    },
                    Box::new(expression),
                );
            } else {
                if self.is("{") {
                    return Err(self.unsupported("MapProjectionOrSubquery"));
                }
                let Some((operator, precedence, width)) = self.binary() else {
                    break;
                };
                if precedence < min {
                    break;
                }
                self.position += width;
                let right = self.expr(precedence + 1)?;
                if matches!(
                    operator,
                    r::Binary::Equal
                        | r::Binary::NotEqual
                        | r::Binary::Less
                        | r::Binary::LessEqual
                        | r::Binary::Greater
                        | r::Binary::GreaterEqual
                ) {
                    let previous = comparison_tail.replace(right.clone());
                    left = match previous {
                        Some(previous) => {
                            let span = Span {
                                start: previous.span.start,
                                end: right.span.end,
                            };
                            let comparison = Expr::new(
                                ExprKind::Binary(operator, Box::new(previous), Box::new(right)),
                                span,
                            )?;
                            ExprKind::Binary(
                                r::Binary::And,
                                Box::new(expression),
                                Box::new(comparison),
                            )
                        }
                        None => ExprKind::Binary(operator, Box::new(expression), Box::new(right)),
                    };
                } else {
                    comparison_tail = None;
                    left = ExprKind::Binary(operator, Box::new(expression), Box::new(right));
                }
            }
            expression = Expr::new(
                left,
                Span {
                    start,
                    end: self.tokens[self.position - 1].span.end,
                },
            )?;
        }
        Ok(expression)
    }

    fn binary(&self) -> Option<(r::Binary, u8, usize)> {
        use r::Binary as B;
        for (s, op, p) in [
            ("OR", B::Or, 1),
            ("XOR", B::Xor, 2),
            ("AND", B::And, 3),
            ("=", B::Equal, 4),
            ("<>", B::NotEqual, 4),
            ("!=", B::NotEqual, 4),
            ("<", B::Less, 4),
            ("<=", B::LessEqual, 4),
            (">", B::Greater, 4),
            (">=", B::GreaterEqual, 4),
            ("IN", B::In, 5),
            ("CONTAINS", B::Contains, 5),
            ("+", B::Add, 6),
            ("-", B::Subtract, 6),
            ("*", B::Multiply, 7),
            ("/", B::Divide, 7),
            ("%", B::Modulo, 7),
            ("^", B::Power, 8),
        ] {
            if self.is(s) {
                return Some((op, p, 1));
            }
        }
        for (s, op) in [("STARTS", B::StartsWith), ("ENDS", B::EndsWith)] {
            if self.is(s)
                && matches!(self.tokens.get(self.position+1).map(|t|&t.kind),Some(Kind::Word(s)) if s.eq_ignore_ascii_case("WITH"))
            {
                return Some((op, 5, 2));
            }
        }
        None
    }
}

fn number_value(text: &str, negative: bool, span: Span) -> Result<r::Value> {
    let (radix, digits) = if text.starts_with("0x") || text.starts_with("0X") {
        (16, &text[2..])
    } else if text.starts_with("0o") || text.starts_with("0O") {
        (8, &text[2..])
    } else {
        (10, text)
    };
    let error = |detail: &str| {
        QueryError::compile(
            "SyntaxError",
            detail,
            "invalid or out-of-range numeric literal",
        )
        .at(span)
    };
    if radix == 10 && text.contains(['.', 'e', 'E']) {
        let value = text
            .parse::<f64>()
            .map_err(|_| error("InvalidNumberLiteral"))?;
        if !value.is_finite() {
            return Err(error("FloatingPointOverflow"));
        }
        return Ok(r::Value::Float(if negative { -value } else { value }));
    }
    if digits.is_empty() || !digits.chars().all(|c| c.is_digit(radix)) {
        return Err(error("InvalidNumberLiteral"));
    }
    let value = u64::from_str_radix(digits, radix).map_err(|_| error("IntegerOverflow"))?;
    if negative && value == 1_u64 << 63 {
        return Ok(r::Value::Integer(i64::MIN));
    }
    let value = i64::try_from(value).map_err(|_| error("IntegerOverflow"))?;
    Ok(r::Value::Integer(if negative { -value } else { value }))
}
