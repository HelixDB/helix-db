#[model("gemini:gemini-embedding-001:RETRIEVAL_DOCUMENT")]
QUERY addClinicalNote (text: String) =>
    note <- AddV<ClinicalNote>(Embed(text), {text: text})
    RETURN note

#[model("gemini:gemini-embedding-001:RETRIEVAL_QUERY")]
QUERY searchClinicalNotes (query: String, k: I64) =>
    notes <- SearchV<ClinicalNote>(Embed(query), k)
    RETURN notes