N::File1 {
    UNIQUE INDEX name: String,
    age: I32,
}


E::File1Edge UNIQUE {
    From: File1,
    To: File1,
}


QUERY file1(name: String) =>
    user <- AddN<File1>({name: name, age: 50})
    RETURN user


QUERY edge(name1: String, name2: String) =>
    user1 <- AddN<File1>({name: name1, age: 50})
    user2 <- AddN<File1>({name: name2, age: 50})
    edge <- AddE<File1Edge>::From(user1)::To(user2)
    RETURN user1

QUERY get(id: ID) =>
    user <- N<File1>(id)
    RETURN user
