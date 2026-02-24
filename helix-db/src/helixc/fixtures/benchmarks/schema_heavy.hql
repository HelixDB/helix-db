schema::1 {
    N::User {
        INDEX email: String,
        name: String,
        age: U32,
        active: Boolean,
        created_at: Date
    }

    N::Team {
        name: String,
        slug: String,
        size: U32
    }

    N::Project {
        name: String,
        status: String,
        budget: F64
    }

    E::BelongsTo {
        From: User,
        To: Team,
        Properties: {
            role: String,
            started_at: Date
        }
    }

    E::WorksOn {
        From: User,
        To: Project,
        Properties: {
            allocation: F64
        }
    }
}

QUERY listUsers() =>
    users <- N<User>
    RETURN users
