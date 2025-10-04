// Test HelixQL queries for path-finding functions

// Test BFS (shortest by hop count)
QUERY shortestPathBFS(from: ID, to: ID) =>
    path <- N<City>(from)::ShortestPathBFS<Road>::To(to)
RETURN path

// Test Dijkstra (shortest by weight)
QUERY shortestPathDijkstra(from: ID, to: ID) =>
    path <- N<City>(from)::ShortestPathDijkstras<Road>(_::{weight})::To(to)
RETURN path

// Test without macro (should default to BFS)
QUERY shortestPathDefault(from: ID, to: ID) =>
    path <- N<City>(from)::ShortestPath<Road>::To(to)
RETURN path