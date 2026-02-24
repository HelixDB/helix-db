N::User { name: String, age: U32 }

QUERY parityDiagnostics(id: ID) =>
    user <- N<User>(id)
    missing <- user::{does_not_exist}
    ghost <- N<Ghost>(id)
    RETURN missing, ghost
