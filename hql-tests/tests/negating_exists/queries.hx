// Start writing your queries here.
//
// You can use the schema to help you write your queries.
//
// Queries take the form:
//     QUERY {query name}({input name}: {input type}) =>
//         {variable} <- {traversal}
//         RETURN {variable}
//
// Example:
//     QUERY GetUserFriends(user_id: ID) =>
//         friends <- N<User>(user_id)::Out<Knows>
//         RETURN friends
//
//
// For more information on how to write queries,
// see the documentation at https://docs.helix-db.com
// or checkout our GitHub at https://github.com/CloudForge-Solutions/openhelix


N::User {}
E::Knows {
    From: User,
    To: User,
}

QUERY GetUsersThatHaveFriends() =>
    has_friends <- N<User>::WHERE(EXISTS(_::Out<Knows>))
    has_no_friends <- N<User>::WHERE(!EXISTS(_::Out<Knows>))
    RETURN has_friends, has_no_friends
