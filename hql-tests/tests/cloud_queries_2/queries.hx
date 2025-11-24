

QUERY ExistsUserByGithubId(github_id: U64) =>
    user_exists <- EXISTS(N<User>({ github_id: github_id }))
    RETURN user_exists
