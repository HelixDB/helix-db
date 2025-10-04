N::User {
    name: String,
    age: U32,
    email: String,
    created_at: Date DEFAULT NOW,
    updated_at: Date DEFAULT NOW,
}

N::Post {
    content: String,
    created_at: Date DEFAULT NOW,
    updated_at: Date DEFAULT NOW,
}

E::Follows {
    From: User,
    To: User,
    Properties: {
        since: Date DEFAULT NOW,
    }
}

E::Created {
    From: User,
    To: Post,
    Properties: {
        created_at: Date DEFAULT NOW,
    }
}


QUERY CreateUser(name: String, age: U32, email: String) =>
    user <- AddN<User>({name: name, age: age, email: email})
    RETURN user


QUERY CreateFollow(follower_id: ID, followed_id: ID) =>
    follower <- N<User>(follower_id)
    followed <- N<User>(followed_id)
    AddE<Follows>::From(follower)::To(followed_id) // don't need to specify the `since` property because it has a default value
    RETURN "success"


QUERY CreatePost(user_id: ID, content: String) =>
    user <- N<User>(user_id)
    post <- AddN<Post>({content: content})
    AddE<Created>::From(user)::To(post) // don't need to specify the `created_at` property because it has a default value
    RETURN post


QUERY GetUsers() =>
    users <- N<User>
    RETURN users

QUERY GetPosts() =>
    posts <- N<Post>
    RETURN posts

QUERY GetPostsByUser(user_id: ID) =>
    posts <- N<User>(user_id)::Out<Created>
    RETURN posts


QUERY GetFollowedUsers(user_id: ID) =>
    followed <- N<User>(user_id)::Out<Follows>
    RETURN followed


QUERY GetFollowedUsersPosts(user_id: ID) =>
    followers <- N<User>(user_id)::Out<Follows>
    posts <- followers::Out<Created>::RANGE(0, 40)
    RETURN posts::{
        post: content,
        creatorID: _::In<Created>::ID,
    }
    