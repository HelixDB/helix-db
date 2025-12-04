QUERY createUser (login: String, userType: String, nodeCreateTime: Date) =>
    user <- AddN<GitHubUser>({
        login: login,
        isEnriched: false,
        userType: userType,
        nodeCreateTime: nodeCreateTime
    })

    RETURN user

QUERY updateUser(login: ID, isEnriched: Boolean, name: String, company: String, location: String, email: String, bio: String, publicRepos: I64, blog: String, twitterUsername: String, followers: I64, following: I64, nodeUpdateTime: Date) =>
    updated <- N<GitHubUser>(login)::UPDATE({
        isEnriched: isEnriched,
        name: name,
        company: company,
        location: location,
        email: email,
        bio: bio,
        publicRepos: publicRepos,
        blog: blog,
        twitterUsername: twitterUsername,
        followers: followers,
        following: following,
        nodeUpdateTime: nodeUpdateTime
    })

    RETURN updated


QUERY createRepo (name: String, fullName: String, ownerType: String, description: String, createdAt: Date, updatedAt: Date, pushedAt: Date, homepage: String, stargazerCount: I64, subscriberCount: I64, language: String, forkCount: I64, openIssueCount: I64, hasWiki: Boolean, hasDiscussions: Boolean, isFork: Boolean, isArchived: Boolean, topics: [String], nodeCreateTime: Date) =>
    repo <- AddN<GitHubRepo>({
        name: name,
        fullName: fullName,
        ownerType: ownerType,
        description: description,
        createdAt: createdAt,
        updatedAt: updatedAt,
        pushedAt: pushedAt,
        homepage: homepage,
        stargazerCount: stargazerCount,
        subscriberCount: subscriberCount,
        language: language,
        forkCount: forkCount,
        openIssueCount: openIssueCount,
        hasWiki: hasWiki,
        hasDiscussions: hasDiscussions,
        isFork: isFork,
        isArchived: isArchived,
        topics: topics,
        nodeCreateTime: nodeCreateTime
    })

    RETURN repo

QUERY createCreatedRepoEdge (fromUserId: String, toRepoId: String, edgeWriteTime: Date) =>
    fromUser <- N<GitHubUser>({login: fromUserId})
    toRepo <- N<GitHubRepo>({fullName: toRepoId})

    edge <- AddE<CreatedRepo>({
        edgeWriteTime: edgeWriteTime
    })::From(fromUser)::To(toRepo)

    RETURN edge

QUERY createStarredRepoEdge (fromUserId: String, toRepoId: String, starredAt: Date, edgeWriteTime: Date) =>
    fromUser <- N<GitHubUser>({login: fromUserId})
    toRepo <- N<GitHubRepo>({fullName: toRepoId})

    edge <- AddE<StarredRepo>({
        starredAt: starredAt,
        edgeWriteTime: edgeWriteTime
    })::From(fromUser)::To(toRepo)

    RETURN edge

QUERY createSubscribedRepoEdge (fromUserId: String, toRepoId: String, edgeWriteTime: Date) =>
    fromUser <- N<GitHubUser>({login: fromUserId})
    toRepo <- N<GitHubRepo>({fullName: toRepoId})

    edge <- AddE<SubscribedRepo>({
        edgeWriteTime: edgeWriteTime
    })::From(fromUser)::To(toRepo)

    RETURN edge

QUERY createFollowsEdge (fromUserId: String, toUserId: String, edgeWriteTime: Date) =>
    fromUser <- N<GitHubUser>({login: fromUserId})
    toUser <- N<GitHubUser>({login: toUserId})

    edge <- AddE<Follows>({
        edgeWriteTime: edgeWriteTime
    })::From(fromUser)::To(toUser)

    RETURN edge

QUERY getAllFollowers (userId: String) =>
    user <- N<GitHubUser>({login: userId})
    followers <- user::In<Follows>

    RETURN followers

QUERY getUser (login: String) =>
    user <- N<GitHubUser>({login: login})

    RETURN user

QUERY getRepo (fullName: String) =>
    repo <- N<GitHubRepo>({fullName: fullName})

    RETURN repo

QUERY getAllUsers () =>
    users <- N<GitHubUser>
    RETURN users

QUERY getAllRepos () =>
    repos <- N<GitHubRepo>
    RETURN repos

QUERY getAllCreatedRepoEdges () =>
    edges <- E<CreatedRepo>
    RETURN edges

QUERY getAllStarredRepoEdges () =>
    edges <- E<StarredRepo>
    RETURN edges

QUERY getAllSubscribedRepoEdges () =>
    edges <- E<SubscribedRepo>
    RETURN edges

QUERY getAllFollowsEdges () =>
    edges <- E<Follows>
    RETURN edges

QUERY RemoveAllFollowsEdges() =>
    DROP E<Follows>
    RETURN "success"

QUERY getUsersWhoAreStargazers () =>
    users <- N<GitHubUser>::WHERE(EXISTS(_::Out<StarredRepo>))
    RETURN users

QUERY getUsersWhoAreSubscribers () =>
    users <- N<GitHubUser>::WHERE(EXISTS(_::Out<SubscribedRepo>))
    RETURN users

QUERY getUsersWhoHaveCreatedRepos () =>
    users <- N<GitHubUser>::WHERE(EXISTS(_::Out<CreatedRepo>))
    RETURN users

QUERY CheckFollowsEdge(fromUserId: String, toUserId: String) =>
    fromUser <- N<GitHubUser>({login: fromUserId})
    toUser <- N<GitHubUser>({login: toUserId})
    result <- fromUser::Out<Follows>::WHERE(_::{login}::EQ(toUser::{login}))
    RETURN result
