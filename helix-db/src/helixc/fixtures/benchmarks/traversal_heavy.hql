N::User { name: String, score: F64 }
N::Team { name: String }
N::Project { name: String, priority: U32 }
E::BelongsTo { From: User, To: Team }
E::Owns { From: Team, To: Project }
E::Follows { From: User, To: User }

QUERY recommendation(user_id: ID, min_score: F64) =>
    user <- N<User>(user_id)
    network <- user::Out<Follows>::Out<Follows>
    teams <- network::Out<BelongsTo>
    projects <- teams::Out<Owns>::ORDER<Desc>(_::{priority})::RANGE(0, 50)
    RETURN { user: user::{id, name}, projects: projects::{id, name, priority} }
