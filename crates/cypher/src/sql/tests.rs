use super::{translate, Mappings};

fn cypher(sql: &str) -> String {
    translate(sql, &Mappings::default())
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .to_string()
}

#[test]
fn select_without_from_returns() {
    assert_eq!(cypher("SELECT 1"), "RETURN 1");
}
