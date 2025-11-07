N::Users {
    fullname: String,
    email: String,
    created_at: Date
}

QUERY ExampleQuery(name: String) =>
    result <- N<Users>::WHERE(_::{fullname}::CONTAINS(name))
    RETURN result

