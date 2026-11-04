//! The SQL translation corpus in `tests/sql/translations.txt`: every case's
//! translation, and whether HelixDB compiles the translated Cypher.
use helix_cypher::sql::{self, Mappings};

/// What one corpus case expects.
enum Expected {
    /// Translated Cypher, and the error compiling it, if any.
    Cypher {
        text: String,
        rejected: Option<(String, String)>,
    },
    /// A translation error's category and detail.
    Error(String, String),
}

struct Case {
    title: String,
    mappings: Mappings,
    sql: String,
    expected: Expected,
}

fn codes(text: &str) -> (String, String) {
    let (category, detail) = text
        .trim()
        .split_once(' ')
        .unwrap_or_else(|| panic!("`{text}` names a category and a detail"));
    (category.to_owned(), detail.trim().to_owned())
}

fn pairs(text: &str) -> impl Iterator<Item = (String, String)> + '_ {
    text.split(',').map(|pair| {
        let (key, value) = pair
            .trim()
            .split_once('=')
            .unwrap_or_else(|| panic!("`{pair}` is key=value"));
        (key.trim().to_owned(), value.trim().to_owned())
    })
}

fn cases() -> Vec<Case> {
    include_str!("../sql/translations.txt")
        .split("\n\n")
        .filter_map(|block| {
            let mut title = None;
            let mut mappings = Mappings::default();
            let mut sql = Vec::new();
            let mut cypher: Option<Vec<&str>> = None;
            let mut rejected = None;
            for line in block.lines() {
                if let Some(comment) = line.strip_prefix('#') {
                    title.get_or_insert_with(|| comment.trim().to_owned());
                } else if let Some(tables) = line.strip_prefix("-- tables:") {
                    mappings.tables.extend(pairs(tables));
                } else if let Some(joins) = line.strip_prefix("-- joins:") {
                    mappings.join_columns.extend(pairs(joins));
                } else if let Some(codes) = line.strip_prefix("!!") {
                    rejected = Some(self::codes(codes));
                } else if let Some(first) = line.strip_prefix("=>") {
                    cypher = Some(vec![first]);
                } else if let Some(lines) = cypher.as_mut() {
                    lines.push(line);
                } else {
                    sql.push(line);
                }
            }
            let cypher = cypher?.join("\n");
            let expected = match cypher.trim().strip_prefix("error:") {
                Some(error) => {
                    let (category, detail) = codes(error);
                    Expected::Error(category, detail)
                }
                None => Expected::Cypher {
                    text: cypher,
                    rejected,
                },
            };
            let sql = sql.join("\n");
            Some(Case {
                title: title.unwrap_or_else(|| sql.lines().next().unwrap_or_default().to_owned()),
                mappings,
                sql,
                expected,
            })
        })
        .collect()
}

#[test]
fn the_corpus_translates_as_expected() {
    let cases = cases();
    assert!(cases.len() > 150, "the corpus has {} cases", cases.len());
    let failures: Vec<String> = cases
        .iter()
        .filter_map(|case| {
            let translated = sql::translate(&case.sql, &case.mappings);
            let failure = match (&case.expected, translated) {
                (Expected::Error(category, detail), Err(error)) => (error.category != *category
                    || error.detail != *detail)
                    .then(|| format!("expected {category} {detail}, got {error:?}")),
                (Expected::Error(..), Ok(statement)) => {
                    Some(format!("expected an error, got `{statement}`"))
                }
                (Expected::Cypher { .. }, Err(error)) => Some(format!("failed: {error:?}")),
                (Expected::Cypher { text, rejected }, Ok(statement)) => {
                    let expected = helix_cypher::parse(text)
                        .unwrap_or_else(|error| {
                            panic!("{}: expected Cypher: {error:?}", case.title)
                        })
                        .to_string();
                    let actual = statement.to_string();
                    // The printed translation is valid Cypher for the same tree.
                    let reprinted = helix_cypher::parse(&actual).map(|s| s.to_string());
                    if reprinted.as_ref() != Ok(&actual) {
                        Some(format!("printed `{actual}` parses as {reprinted:?}"))
                    } else if actual != expected {
                        Some(format!("expected `{expected}`\n     got `{actual}`"))
                    } else {
                        match (helix_cypher::resolve(&statement), rejected) {
                            (Ok(_), None) => None,
                            (Err(error), Some((category, detail)))
                                if error.category == *category && error.detail == *detail =>
                            {
                                None
                            }
                            (Ok(_), Some(_)) => Some("compiled, but should be rejected".to_owned()),
                            (Err(error), _) => Some(format!("does not compile: {error:?}")),
                        }
                    }
                }
            };
            failure.map(|failure| {
                format!(
                    "{}\n  {}\n  {failure}",
                    case.title,
                    case.sql.replace('\n', " ")
                )
            })
        })
        .collect();
    assert!(
        failures.is_empty(),
        "{} of {} cases failed:\n\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n\n")
    );
}
