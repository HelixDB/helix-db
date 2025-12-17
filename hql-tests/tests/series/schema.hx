N::User {
    phone: String,
    email: String,
    bio: String,
    age: I32,
    location: String,
    profilePic: String,
    color: String,
    elo: F64,
    sender: String
}

N::Metadata {
    created_ts: Date,
    last_updated_ts: Date,
    archetype: Boolean,
    referredBy: String
}

N::Name {
    first: String,
    last: String
}

N::MetadataNotes {
    aiScore: I64,
    userScore: I64,
    text: String,
    flagged: Boolean
}

N::WarmConnect {
    name: String,
    email: String
}

N::LinkedinInfo {
    url: String
}

N::LinkedinContent {
    name: String,
    email: String,
    linkedin_url: String,
    full_name: String,
    first_name: String,
    last_name: String,
    public_id: String,
    profile_picture: String,
    background_picture: String,
    current_position: String,
    summary: String,
    industry: String,
    region: String,
    country: String,
    country_code: String,
    connection_count: I64,
    follower_count: I64,
    languages: [String],
    skills: [String],
    certifications: [String],
    position_start_date: String,
    position_end_date: String,
    extracted_at: Date,
    data_source: String
}

N::LinkedinWebsite {
    url: String,
    category: String
}

N::LinkedinExperience {
    company: String,
    title: String,
    field: String,
    date_start: String,
    date_end: String,
    location: String,
    description: String
}

N::LinkedinCompany {
    name: String,
    domain: String,
    industry: String,
    staff_count: I64,
    founded: I64,
    website: String,
    headquarters: String,
    description: String,
    specialties: [String]
}

N::LinkedinEducation {
    school: String,
    field: String,
    title: String,
    date_start: String,
    date_end: String,
    location: String,
    description: String
}

E::User_to_Name {
    From: User,
    To: Name,
    Properties: {
        first: String
    }
}

E::User_to_Metadata {
    From: User,
    To: Metadata,
    Properties: {
        created_ts: Date,
        last_updated_ts: Date
    }
}

E::User_to_EmbeddedBio {
    From: User,
    To: EmbeddedBio,
    Properties: {
    }
}

E::Metadata_to_MetadataNotes {
    From: Metadata,
    To: MetadataNotes,
    Properties: {
    }
}

E::Metadata_to_LinkedinInfo {
    From: Metadata,
    To: LinkedinInfo,
    Properties: {
        url: String
    }
}

E::Metadata_to_WarmConnect {
    From: Metadata,
    To: WarmConnect,
    Properties: {
    }
}

E::LinkedinInfo_to_LinkedinContent {
    From: LinkedinInfo,
    To: LinkedinContent,
    Properties: {
    }
}

E::LinkedinContent_to_LinkedinWebsite {
    From: LinkedinContent,
    To: LinkedinWebsite,
    Properties: {
    }
}

E::LinkedinContent_to_LinkedinExperience {
    From: LinkedinContent,
    To: LinkedinExperience,
    Properties: {
    }
}

E::LinkedinContent_to_LinkedinCompany {
    From: LinkedinContent,
    To: LinkedinCompany,
    Properties: {
    }
}

E::LinkedinContent_to_LinkedinEducation {
    From: LinkedinContent,
    To: LinkedinEducation,
    Properties: {
    }
}

E::LinkedinContent_to_CurrentCompany {
    From: LinkedinContent,
    To: LinkedinCompany,
    Properties: {
    }
}

V::EmbeddedBio {
    bio: [F32]
}
