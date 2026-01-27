// Social Network guide example - Full queries

// Create a user
QUERY createUser (name: String, age: U32, email: String) =>
    user <- AddN<User>({name: name, age: age, email: email})
    RETURN user

// Create a follow relationship
QUERY createFollow (follower_id: ID, followed_id: ID) =>
    follower <- N<User>(follower_id)
    followed <- N<User>(followed_id)
    AddE<Follows>::From(follower)::To(followed)
    RETURN "success"

// Create a post
QUERY createPost (user_id: ID, content: String) =>
    user <- N<User>(user_id)
    post <- AddN<Post>({content: content})
    AddE<Created>::From(user)::To(post)
    RETURN post

// Get all users
QUERY getUsers () =>
    users <- N<User>
    RETURN users

// Get all posts
QUERY getPosts () =>
    posts <- N<Post>
    RETURN posts

// Get posts by a specific user
QUERY getPostsByUser (user_id: ID) =>
    posts <- N<User>(user_id)::Out<Created>
    RETURN posts

// Get all users that a user follows
QUERY getFollowedUsers (user_id: ID) =>
    followed <- N<User>(user_id)::Out<Follows>
    RETURN followed

// Get posts from followed users with property remapping and RANGE
QUERY getFollowedUsersPosts (user_id: ID) =>
    followers <- N<User>(user_id)::Out<Follows>
    posts <- followers::Out<Created>::RANGE(0, 40)
    RETURN posts::{
        post: _::{content},
        creatorID: _::In<Created>::ID,
    }
