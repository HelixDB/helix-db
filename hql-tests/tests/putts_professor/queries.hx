// Create Professor Node
QUERY create_professor(name: String, title: String, page: String, bio: String ) =>
    professor <- AddN<Professor>({ name: name, title: title, page: page, bio: bio })
    RETURN professor

// Create Department Node
QUERY create_department(name: String) =>
    department <- AddN<Department>({ name: name })
    RETURN department

// Create University Node
QUERY create_university(name: String) =>
    university <- AddN<University>({ name: name })
    RETURN university

// Create Lab Node
QUERY create_lab(name: String, research_focus: String) =>
    lab <- AddN<Lab>({ name: name, research_focus: research_focus })
    RETURN lab

// Create Research Area Node
QUERY create_research_area(area: String) =>
    research_area <- AddN<ResearchArea>({ research_area: area })
    RETURN research_area

// Link Professor to Department
QUERY link_professor_to_department(professor_id: ID, department_id: ID) =>
    professor <- N<Professor>(professor_id)
    department <- N<Department>(department_id)
    edge <- AddE<HasDepartment>::From(professor)::To(department)
    RETURN edge


// Link Professor to University
QUERY link_professor_to_university(professor_id: ID, university_id: ID) =>
    professor <- N<Professor>(professor_id)
    university <- N<University>(university_id)
    edge <- AddE<HasUniversity>::From(professor)::To(university)
    RETURN edge


// Link Professor to Lab
QUERY link_professor_to_lab(professor_id: ID, lab_id: ID) =>
    professor <- N<Professor>(professor_id)
    lab <- N<Lab>(lab_id)
    edge <- AddE<HasLab>::From(professor)::To(lab)
    RETURN edge

// Link Professor to Research Area
QUERY link_professor_to_research_area(professor_id: ID, research_area_id: ID) =>
    professor <- N<Professor>(professor_id)
    research_area <- N<ResearchArea>(research_area_id)
    edge <- AddE<HasResearchArea>::From(professor)::To(research_area)
    RETURN edge

// Search Similar Professors based on Research Area + Description Embedding
QUERY search_similar_professors_by_research_area_and_description(query_vector: [F32], k: I64) =>
    vecs <- SearchV<ResearchAreaAndDescriptionEmbedding>(query_vector, k)
    professors <- vecs::In<HasResearchAreaAndDescriptionEmbedding>
    RETURN professors

// Get the actual string data of a professor's research area given professor ID
QUERY get_professor_research_areas_with_descriptions(professor_id: ID) =>
    research_areas <- N<Professor>(professor_id)::Out<HasResearchAreaAndDescriptionEmbedding>::{areas_and_descriptions: areas_and_descriptions}
    RETURN research_areas

QUERY create_research_area_embedding(professor_id: ID, areas_and_descriptions: String, vector: [F32]) =>
    professor <- N<Professor>(professor_id)
    research_area <- AddV<ResearchAreaAndDescriptionEmbedding>(vector, { areas_and_descriptions: areas_and_descriptions })
    edge <- AddE<HasResearchAreaAndDescriptionEmbedding>::From(professor)::To(research_area)
    RETURN research_area


// GET Queries //

QUERY get_professors_by_university_name(university_name: String) =>
    professors <- N<Professor>::Out<HasUniversity>::WHERE(_::{name}::EQ(university_name))
    RETURN professors

QUERY get_professor_by_research_area_name(research_area_name: String) =>
    professors <- N<Professor>::Out<HasResearchArea>::WHERE(_::{research_area}::EQ(research_area_name))
    RETURN professors

QUERY get_professors_by_department_name(department_name: String) =>
    professors <- N<Professor>::Out<HasDepartment>::WHERE(_::{name}::EQ(department_name))
    RETURN professors
