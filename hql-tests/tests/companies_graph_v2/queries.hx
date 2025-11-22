// ------------------------------ NODE OPERATIONS ---------------------------
QUERY GetCompanies() =>
    companies <- N<Company>
    RETURN companies

QUERY GetCompany(company_number: String) =>
    company <- N<Company>({company_number: company_number})
    RETURN company

QUERY CreateCompany(company_name: String, company_number: String, total_docs: I32) =>
    company <- AddN<Company>({
        company_name: company_name,
        company_number: company_number,
        total_docs: total_docs,
        ingested_docs: 0
    })
    RETURN company

// keep track of how many documents have been processed for a company
QUERY UpdateCompany(company_number: String, ingested_docs: I32) =>
    company <- N<Company>({company_number: company_number})::UPDATE({
        ingested_docs: ingested_docs
    })
    RETURN company

QUERY DeleteCompany(company_number: String) =>
    DROP N<Company>({company_number: company_number})::Out<DocumentEdge>
    DROP N<Company>({company_number: company_number})
    RETURN "success"

// ------------------------------ EDGE OPERATIONS --------------------------

QUERY GetDocumentEdges(company_number: String) =>
    c <- N<Company>({company_number: company_number})
    edges <- c::OutE<DocumentEdge>
    RETURN edges


// ─── filing / embedding helpers ───────────────────────────────

QUERY AddEmbeddingsToCompany(
    company_number: String,
    embeddings_data: [{
        vector: [F32],
        text: String,
        chunk_id: String,
        page_number: I32,
        reference: String,
        ch_file_id: String,
        category: String,
        subcategory: String,
        date1: String,
        date2: String,
        source: String,
        description: String
    }]
) =>
    c <- N<Company>({company_number: company_number})
    FOR { vector, text, chunk_id, page_number, reference, ch_file_id, category, subcategory, date1, date2, source, description } IN embeddings_data {
        embedding <- AddV<DocumentEmbedding>(
            vector, {
                text: text,
                chunk_id: chunk_id,
                page_number: page_number,
                reference: reference,
                source_link: source,
                source_date: date1
        })

            edges <- AddE<DocumentEdge>({
                ch_file_id: ch_file_id,
                category: category,
                subcategory: subcategory,
                date: date2,
                description: description,
                source_link: source
            })::From(c)::To(embedding)
    }

    RETURN "success"


QUERY GetAllCompanyEmbeddings(company_number: String) =>
    c <- N<Company>({company_number: company_number})
    embeddings <- c::Out<DocumentEdge>
    RETURN embeddings

QUERY CompanyEmbeddingSearch(company_number: String, query: [F32], k: I32) =>
    c <- N<Company>({company_number: company_number})::OutE<DocumentEdge>::ToV
    embedding_search <- c::SearchV<DocumentEmbedding>(query, k)
    RETURN embedding_search

// ---------------------- FOR TESTING ---------------------------------
//  tmp function for testing helix
QUERY AddVector(vector: [F32], text: String, chunk_id: String, page_number: I32, reference: String) =>
    embedding <- AddV<DocumentEmbedding>(vector, {text: text, chunk_id: chunk_id, page_number: page_number, reference: reference})
    RETURN embedding

//  tmp function for testing helix
QUERY SearchVector(query: [F32], k: I32) =>
    embedding_search <- SearchV<DocumentEmbedding>(query, k)
    RETURN embedding_search

QUERY GetVectorsByCategory(company_number: String, value: String) =>
    // Get outgoing edges first
    edges <- N<Company>({company_number:company_number})::OutE<DocumentEdge>::WHERE(_::{category}::EQ(value))
    // Then get the vectors they connect to
    vectors <- edges::ToV
    RETURN vectors

QUERY GetVectorsBySourceLink(company_number: String, source_link: String) =>
    vectors <- N<Company>({company_number:company_number})::OutE<DocumentEdge>::ToV::WHERE(_::{source_link}::EQ(source_link))
    RETURN vectors


QUERY GetVectorsBySourceLinkAndPageRange(company_number: String, source_link: String, start_page: I32, end_page: I32) =>
    edges <- N<Company>({company_number:company_number})::OutE<DocumentEdge>
    vectors <- edges::ToV::WHERE(
                AND(
                    _::{page_number}::GTE(start_page),
                    _::{page_number}::LTE(end_page),
                    _::{source_link}::EQ(source_link)
                )
            )
    RETURN vectors
