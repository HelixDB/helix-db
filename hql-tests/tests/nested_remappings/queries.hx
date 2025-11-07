N::User {
    name: String,
    age: U8,
    email: String
}

N::Post {
    title: String,
    content: String
}

E::HasPost {
    From: User,
    To: Post
}

QUERY GetUserPosts (user_id: ID) =>
    user <- N<User>(user_id)
    posts <- user::Out<HasPost>
    RETURN user::|usr|{
        posts: posts::{
            postID: ID,
            creatorID: usr::ID,
            creatorName: usr::{name},
            ..
        }
    }

QUERY GetUserPostsWorking2 (user_id: ID) =>
    user <- N<User>(user_id)
    posts <- user::Out<HasPost>
    RETURN user::{
            creatorID: id, 
            creatorName: name,
    }

QUERY GetUserPostsWorking (user_id: ID) =>
    user <- N<User>(user_id)
    posts <- user::Out<HasPost>
    RETURN user::{
            id, 
            name
    }

QUERY CreateUser (name: String, age: U8, email: String) =>
    user <- AddN<User>({
        name: name,
        age: age,
        email: email
    })
    RETURN user

QUERY CreatePost (user_id: ID, title: String, content: String) =>
    user <- N<User>(user_id)
    post <- AddN<Post>({
        title: title,
        content: content
    })
    AddE<HasPost>::From(user)::To(post)
    RETURN post