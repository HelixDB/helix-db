

QUERY add(name: String) =>
    user <- AddN<File1>({name: name, age: 50})
    RETURN user

QUERY get(name: String) =>
    user <- N<File1>({name: name})
    RETURN user
