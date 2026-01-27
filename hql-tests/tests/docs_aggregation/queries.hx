// Aggregation documentation examples

// Example 1: Group users by age
QUERY GroupUsersByAge () =>
    users <- N<User>
    RETURN users::GROUP_BY(age)

// Example 2: Aggregate users by age
QUERY AggregateUsersByAge () =>
    users <- N<User>
    RETURN users::AGGREGATE_BY(age)

// Helper query to create users
QUERY CreateUser (name: String, age: U8, email: String) =>
    user <- AddN<User>({
        name: name,
        age: age,
        email: email
    })
    RETURN user
