N::User { name: String, email: String, city: String }
N::Post { title: String, body: String }
E::Authored { From: User, To: Post, Properties: { created_at: Date } }

QUERY dashboard(user_id: ID) =>
    user <- N<User>(user_id)
    posts <- user::Out<Authored>
    RETURN {
        profile: {
            id: user::ID,
            name: user::{name},
            contact: {
                email: user::{email},
                city: user::{city}
            }
        },
        posts: [
            {
                id: posts::ID,
                title: posts::{title},
                body: posts::{body}
            }
        ]
    }
