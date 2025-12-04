N::User {
    INDEX github_id: U64,
    github_login: String,
    github_name: String DEFAULT "",
    github_email: String DEFAULT "",
    created_at: Date DEFAULT NOW,
    updated_at: Date DEFAULT NOW,
}

N::Cluster {
    INDEX railway_project_id: String,
    project_name: String,
    railway_region: String DEFAULT "us-east4-eqdc4a",
    db_url: String DEFAULT "",
    created_at: Date DEFAULT NOW,
    updated_at: Date DEFAULT NOW,
}

N::Instance {
    railway_service_id: String,
    railway_environment_id: String,
    instance_type: String,
    storage_gb: U64,
    ram_gb: U64,
    created_at: Date DEFAULT NOW,
    updated_at: Date DEFAULT NOW,
}

E::CreatedCluster {
    From: User,
    To: Cluster,
}

E::HasInstance {
    From: Cluster,
    To: Instance,
}

N::ApiKey {
    unkey_key_id: String,
}

E::CreatedApiKey {
    From: User,
    To: ApiKey,
}