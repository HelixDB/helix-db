use crate::helixc::parser::{
    HelixParser, ParserError, Rule,
    location::HasLoc,
    types::{EdgeConnection, Expression, IdType},
};
use pest::iterators::{Pair, Pairs};

impl HelixParser {
    pub(super) fn parse_id_args(&self, pair: Pair<Rule>) -> Result<Option<IdType>, ParserError> {
        let p = pair
            .into_inner()
            .next()
            .ok_or_else(|| ParserError::from("Missing ID"))?;
        match p.as_rule() {
            Rule::identifier => Ok(Some(IdType::Identifier {
                value: p.as_str().to_string(),
                loc: p.loc(),
            })),
            Rule::string_literal | Rule::inner_string => Ok(Some(IdType::Literal {
                value: p.as_str().to_string(),
                loc: p.loc(),
            })),
            _ => Err(ParserError::from(format!(
                "Unexpected rule in parse_id_args: {:?}",
                p.as_rule()
            ))),
        }
    }
    pub(super) fn parse_vec_literal(&self, pair: Pair<Rule>) -> Result<Vec<f64>, ParserError> {
        let pairs = pair.into_inner();
        let mut vec = Vec::new();
        for p in pairs {
            vec.push(
                p.as_str()
                    .parse::<f64>()
                    .map_err(|_| ParserError::from("Invalid float value"))?,
            );
        }
        Ok(vec)
    }

    pub(super) fn parse_array_literal(
        &self,
        pair: Pair<Rule>,
    ) -> Result<Vec<Expression>, ParserError> {
        pair.into_inner()
            .map(|p| self.parse_expression(p))
            .collect()
    }

    pub(super) fn parse_string_literal(&self, pair: Pair<Rule>) -> Result<String, ParserError> {
        let inner = pair
            .into_inner()
            .next()
            .ok_or_else(|| ParserError::from("Empty string literal"))?;

        let mut literal = inner.as_str().to_string();
        literal.retain(|c| c != '"');
        Ok(literal)
    }

    pub(super) fn parse_to_from(&self, pair: Pair<Rule>) -> Result<EdgeConnection, ParserError> {
        let pairs = pair.clone().into_inner();
        let mut from_id = None;
        let mut to_id = None;
        // println!("pairs: {:?}", pairs);
        for p in pairs {
            match p.as_rule() {
                Rule::from => {
                    from_id = self.parse_id_args(p.into_inner().next().unwrap())?;
                }
                Rule::to => {
                    to_id = self.parse_id_args(p.into_inner().next().unwrap())?;
                }
                _ => {
                    return Err(ParserError::from(format!(
                        "Unexpected rule in parse_to_from: {:?}",
                        p.as_rule()
                    )));
                }
            }
        }
        Ok(EdgeConnection {
            from_id,
            to_id,
            loc: pair.loc(),
        })
    }
}

pub trait PairTools<'a> {
    /// Equivalent to into_inner().next()
    #[track_caller]
    fn try_inner_next(self) -> Result<Pair<'a, Rule>, ParserError>;
}

pub trait PairsTools<'a> {
    /// Equivalent to next()
    fn try_next(&mut self) -> Result<Pair<'a, Rule>, ParserError>;

    /// Equivalent to next().into_inner()
    fn try_next_inner(&mut self) -> Result<Pairs<'a, Rule>, ParserError>;
}

impl<'a> PairTools<'a> for Pair<'a, Rule> {
    #[track_caller]
    fn try_inner_next(self) -> Result<Pair<'a, Rule>, ParserError> {
        let err_msg = format!("Expected inner next got {self:?}");
        self.into_inner()
            .next()
            .ok_or_else(|| ParserError::from(err_msg))
    }
}

impl<'a> PairTools<'a> for Result<Pair<'a, Rule>, ParserError> {
    #[track_caller]
    fn try_inner_next(self) -> Result<Pair<'a, Rule>, ParserError> {
        match self {
            Ok(pair) => pair
                .into_inner()
                .next()
                .ok_or_else(|| ParserError::from("Expected inner next")),
            Err(e) => Err(e),
        }
    }
}

impl<'a> PairsTools<'a> for Pairs<'a, Rule> {
    fn try_next(&mut self) -> Result<Pair<'a, Rule>, ParserError> {
        self.next()
            .ok_or_else(|| ParserError::from("Expected next"))
    }

    fn try_next_inner(&mut self) -> Result<Pairs<'a, Rule>, ParserError> {
        match self.next() {
            Some(pair) => Ok(pair.into_inner()),
            None => Err(ParserError::from("Expected next inner")),
        }
    }
}

impl<'a> PairsTools<'a> for Result<Pairs<'a, Rule>, ParserError> {
    fn try_next(&mut self) -> Result<Pair<'a, Rule>, ParserError> {
        match self {
            Ok(pairs) => pairs.try_next(),
            Err(e) => Err(e.clone()),
        }
    }

    fn try_next_inner(&mut self) -> Result<Pairs<'a, Rule>, ParserError> {
        match self {
            Ok(pairs) => pairs.try_next_inner(),
            Err(e) => Err(e.clone()),
        }
    }
}
