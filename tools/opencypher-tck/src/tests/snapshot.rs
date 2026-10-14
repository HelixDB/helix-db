use super::*;
#[test]
fn changes_count_property_replacement_and_distinct_labels() {
    let mut before = Snapshot::default();
    before.nodes.extend([1, 2]);
    before.labels.insert("A".into());
    before.properties.insert((true, 1, "p".into(), "1".into()));
    let mut after = Snapshot::default();
    after.nodes.extend([1, 2]);
    after.labels.insert("A".into());
    after.properties.insert((true, 1, "p".into(), "2".into()));
    let changes = before.changes(&after);
    assert_eq!(changes["+labels"], 0);
    assert_eq!(changes["+properties"], 1);
    assert_eq!(changes["-properties"], 1);
}
