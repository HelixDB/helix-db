use super::document::TextIndexedDocument;

#[derive(Debug, Clone, PartialEq)]
pub(super) enum TextIndexChange {
    None,
    Remove {
        old: TextIndexedDocument,
    },
    Upsert {
        new: TextIndexedDocument,
    },
    Replace {
        old: TextIndexedDocument,
        new: TextIndexedDocument,
    },
}

impl TextIndexChange {
    pub(super) fn from_documents(
        old: Option<TextIndexedDocument>,
        new: Option<TextIndexedDocument>,
    ) -> Self {
        match (old, new) {
            (None, None) => Self::None,
            (None, Some(new)) => Self::Upsert { new },
            (Some(old), None) => Self::Remove { old },
            (Some(old), Some(new)) if old.same_indexed_content(&new) => Self::None,
            (Some(old), Some(new)) => Self::Replace { old, new },
        }
    }
}

#[cfg(test)]
fn text_document(index_name: &str, text: &str) -> TextIndexedDocument {
    TextIndexedDocument {
        index_name: index_name.to_string(),
        input: crate::search::text::TextDocumentInput::new(42, text.to_string()),
        logical_partition_identity: vec![0],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn change_encodes_noop_upsert_remove_and_replace() {
        let old = text_document("fts:n:Doc:body", "old");
        let same = text_document("fts:n:Doc:body", "old");
        let updated = text_document("fts:n:Doc:body", "new");
        let moved = text_document("ftsmt:n:Doc:body:tenant", "old");

        assert_eq!(
            TextIndexChange::from_documents(None, None),
            TextIndexChange::None
        );
        assert_eq!(
            TextIndexChange::from_documents(None, Some(updated.clone())),
            TextIndexChange::Upsert {
                new: updated.clone()
            }
        );
        assert_eq!(
            TextIndexChange::from_documents(Some(old.clone()), None),
            TextIndexChange::Remove { old: old.clone() }
        );
        assert_eq!(
            TextIndexChange::from_documents(Some(old.clone()), Some(same)),
            TextIndexChange::None
        );
        assert_eq!(
            TextIndexChange::from_documents(Some(old.clone()), Some(updated.clone())),
            TextIndexChange::Replace {
                old: old.clone(),
                new: updated
            }
        );
        assert_eq!(
            TextIndexChange::from_documents(Some(old.clone()), Some(moved.clone())),
            TextIndexChange::Replace { old, new: moved }
        );
    }
}
