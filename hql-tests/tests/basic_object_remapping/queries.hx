
N::User {
    name: String,
    age: I32,
}


E::Knows {
    From: User,
    To: User,
}


QUERY user() =>
    user <- AddN<User>({name: "John", age: 20})
    user2 <- N<User>::Out<Knows>
    RETURN user::{
        username: name,
        age: 21
    }








