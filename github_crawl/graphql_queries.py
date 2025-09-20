"""GraphQL queries used by the crawler."""

REPOSITORY_COUNT_QUERY = """
query ($query: String!) {
  rateLimit {
    cost
    remaining
    resetAt
  }
  search(query: $query, type: REPOSITORY, first: 1) {
    repositoryCount
  }
}
"""

REPOSITORY_SEARCH_QUERY = """
query ($query: String!, $first: Int!, $after: String) {
  rateLimit {
    cost
    remaining
    resetAt
  }
  search(query: $query, type: REPOSITORY, first: $first, after: $after) {
    repositoryCount
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      ...RepositoryFields
    }
  }
}

fragment RepositoryFields on Repository {
  id
  databaseId
  name
  nameWithOwner
  description
  stargazerCount
  forkCount
  isPrivate
  isFork
  isArchived
  createdAt
  updatedAt
  pushedAt
  owner {
    login
    __typename
  }
  watchers {
    totalCount
  }
  issues(states: OPEN) {
    totalCount
  }
  primaryLanguage {
    name
  }
}
"""

__all__ = ["REPOSITORY_COUNT_QUERY", "REPOSITORY_SEARCH_QUERY"]
