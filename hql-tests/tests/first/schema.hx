N::User {
    name: String,
    age: I32,
}

N::App {
    name: String,
    description: String,
    created_at: Date,
    favorite: Boolean,
    archived: Boolean,
}

E::User_Has_Access_To {
    From: User,
    To: App,
    Properties: {
       modified_at: Date,
    }
}
