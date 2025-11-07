QUERY GetUsers() =>
    users <- N<User>::FIRST
    RETURN users

QUERY GetAppsByUserId(user_id: ID) => 
    user <- N<User>(user_id)
    apps <- user::Out<User_Has_Access_To>
    RETURN apps::{
        access_modified_at: _::InE<User_Has_Access_To>::{modified_at}::FIRST,
        name,
        description, 
        created_at,
        favorite,
        archived,
        id,
    } 