// Conditionals documentation examples

// Example 1: Basic filtering with WHERE using GT
QUERY GetAdultUsers () =>
    adult_users <- N<User>::WHERE(_::{age}::GT(18))
    RETURN adult_users

// Example 2: String and equality filtering
QUERY GetActiveUsers (status: String) =>
    active_users <- N<User>::WHERE(_::{status}::EQ(status))
    RETURN active_users

// Example 3: Using EXISTS for relationship filtering
QUERY GetUsersWithFollowers () =>
    users <- N<User>::WHERE(EXISTS(_::In<Follows>))
    RETURN users

// Additional comparison examples
QUERY GetUsersUnder30 () =>
    users <- N<User>::WHERE(_::{age}::LT(30))
    RETURN users

QUERY GetUsersAtLeast25 () =>
    users <- N<User>::WHERE(_::{age}::GTE(25))
    RETURN users

QUERY GetUsersAtMost40 () =>
    users <- N<User>::WHERE(_::{age}::LTE(40))
    RETURN users

QUERY GetUsersNotAdmin (status: String) =>
    users <- N<User>::WHERE(_::{status}::NEQ(status))
    RETURN users

// Helper queries
QUERY CreateUser (name: String, age: U8, email: String, status: String) =>
    user <- AddN<User>({
        name: name,
        age: age,
        email: email,
        status: status
    })
    RETURN user

QUERY CreateFollow (follower_id: ID, following_id: ID) =>
    follower <- N<User>(follower_id)
    following <- N<User>(following_id)
    AddE<Follows>::From(follower)::To(following)
    RETURN "Success"
