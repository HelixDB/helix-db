// queries.hx
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
//     QUERY GetUserFriends(user_id: String) =>
//         friends <- N<User>(user_id)::Out<Knows>
//         RETURN friends
//
//
// For more information on how to write queries,
// see the documentation at https://docs.helix-db.com
// or checkout our GitHub at https://github.com/HelixDB/helix-db

QUERY createUser(name: String, email: String) =>
  user <- AddN<User>({
    name: name,
    email: email,
  })
  RETURN user

QUERY getUserByEmail(email: String) =>
  user <- N<User>({email: email})
  RETURN user