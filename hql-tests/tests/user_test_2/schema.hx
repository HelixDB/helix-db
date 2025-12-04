N::GitHubUser {
    INDEX login: String,
    isEnriched: Boolean,
    userType: String,
    name: String DEFAULT "",
    company: String DEFAULT "",
    location: String DEFAULT "",
    email: String DEFAULT "",
    bio: String DEFAULT "",
    publicRepos: I64 DEFAULT 0,
    blog: String DEFAULT "",
    twitterUsername: String DEFAULT "",
    followers: I64 DEFAULT 0,
    following: I64 DEFAULT 0,
    nodeCreateTime: Date,
    nodeUpdateTime: Date DEFAULT "2000-01-01T00:00:00Z",
}

N::GitHubRepo {
    name: String,
    INDEX fullName: String,
    ownerType: String,
    description: String,
    createdAt: Date,
    updatedAt: Date,
    pushedAt: Date,
    homepage: String,
    language: String,
    stargazerCount: I64,
    subscriberCount: I64,
    forkCount: I64,
    openIssueCount: I64,
    hasWiki: Boolean,
    hasDiscussions: Boolean,
    isFork: Boolean,
    isArchived: Boolean,
    topics: [String],
    nodeCreateTime: Date,
}

E::CreatedRepo {
    From: GitHubUser,
    To: GitHubRepo,
    Properties: {
        edgeWriteTime: Date,
    }
}

E::StarredRepo {
    From: GitHubUser,
    To: GitHubRepo,
    Properties: {
        starredAt: Date,
        edgeWriteTime: Date,
    }
}

E::SubscribedRepo {
    From: GitHubUser,
    To: GitHubRepo,
    Properties: {
        edgeWriteTime: Date,
    }
}

E::ContributedTo {
    From: GitHubUser,
    To: GitHubRepo,
    Properties: {
        contributionsCount: I64,
        edgeWriteTime: Date,
    }
}

E::AffiliatedWith {
    From: GitHubUser,
    To: GitHubUser,
    Properties: {
        edgeWriteTime: Date,
    }
}

E::ForkedRepo {
    From: GitHubRepo,
    To: GitHubRepo,
    Properties: {
        edgeWriteTime: Date,
    }
}

E::Follows {
    From: GitHubUser,
    To: GitHubUser,
    Properties: {
        edgeWriteTime: Date,
    }
}

E::DependsOn {
    From: GitHubRepo,
    To: GitHubRepo,
    Properties: {
        edgeWriteTime: Date,
    }
}
