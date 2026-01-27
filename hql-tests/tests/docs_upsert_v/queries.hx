// UpsertV documentation examples

// Example 1: Basic vector upsert with properties
QUERY UpsertDoc(vector: [F64], content: String) =>
    existing <- V<Document>::WHERE(_::{content}::EQ(content))
    doc <- existing::UpsertV(vector, {content: content})
    RETURN doc

// Example 2: Upsert vector using the Embed function
QUERY UpsertDocEmbed(text: String) =>
    existing <- V<Document>::WHERE(_::{content}::EQ(text))
    doc <- existing::UpsertV(Embed(text), {content: text})
    RETURN doc

// Example 3: Complex operation with nodes, edges, and vectors
QUERY ComplexUpsertOperation(
    person_name: String,
    person_age: U32,
    company_name: String,
    position: String,
    resume_content: String
) =>
    existing_person <- N<Person>::WHERE(_::{name}::EQ(person_name))
    person <- existing_person::UpsertN({name: person_name, age: person_age})
    existing_company <- N<Company>::WHERE(_::{name}::EQ(company_name))
    company <- existing_company::UpsertN({name: company_name})
    existing_edge <- E<WorksAt>
    edge <- existing_edge::UpsertE({position: position})::From(person)::To(company)
    existing_resume <- V<Resume>::WHERE(_::{content}::EQ(resume_content))
    resume <- existing_resume::UpsertV(Embed(resume_content), {content: resume_content})
    RETURN person

// Helper query to get persons
QUERY GetPerson(name: String) =>
    person <- N<Person>::WHERE(_::{name}::EQ(name))
    RETURN person

// Helper query to get all documents
QUERY GetAllDocuments() =>
    docs <- V<Document>
    RETURN docs
