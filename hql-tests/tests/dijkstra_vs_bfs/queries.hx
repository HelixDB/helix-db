// Test queries comparing BFS and Dijkstra algorithms

// ─── Location/Route Tests ───────────────────────────────────────

// BFS: Finds path with minimum number of hops
QUERY routeBFS(start: ID, end: ID) =>
    path <- N<Location>(start)::ShortestPathBFS<Route>::To(end)
RETURN path

// Dijkstra: Finds path with minimum distance
QUERY routeDijkstra(start: ID, end: ID) =>
    path <- N<Location>(start)::ShortestPathDijkstras<Route>(_::{distance})::To(end)
RETURN path

// Default (should use BFS)
QUERY routeDefault(start: ID, end: ID) =>
    path <- N<Location>(start)::ShortestPath<Route>::To(end)
RETURN path

// Test with From instead of To
QUERY routeDijkstraFrom(start: ID, end: ID) =>
    path <- N<Location>(end)::ShortestPathDijkstras<Route>(_::{distance})::From(start)
RETURN path

// ─── Flight Network Tests ───────────────────────────────────────

// Compare flight paths by time vs hop count
QUERY flightBFS(origin: ID, destination: ID) =>
    path <- N<Location>(origin)::ShortestPathBFS<FlightPath>::To(destination)
RETURN path

QUERY flightDijkstra(origin: ID, destination: ID) =>
    path <- N<Location>(origin)::ShortestPathDijkstras<FlightPath>(_::{flight_time})::To(destination)
RETURN path

// ─── Social Network Tests (BFS is ideal for these) ─────────────

// Find shortest connection between people
QUERY socialBFS(person1: ID, person2: ID) =>
    path <- N<Person>(person1)::ShortestPathBFS<Follows>::To(person2)
RETURN path

// Dijkstra on social network (uses interaction_score as weight)
QUERY socialDijkstra(person1: ID, person2: ID) =>
    path <- N<Person>(person1)::ShortestPathDijkstras<Follows>(_::{interaction_score})::To(person2)
RETURN path

// ─── Transportation Network Tests ────────────────────────────────

// Train routes only
QUERY trainBFS(from: ID, to: ID) =>
    path <- N<Station>(from)::ShortestPathBFS<TrainRoute>::To(to)
RETURN path

QUERY trainDijkstra(from: ID, to: ID) =>
    path <- N<Station>(from)::ShortestPathDijkstras<TrainRoute>(_::{duration_minutes})::To(to)
RETURN path

// Bus routes only  
QUERY busBFS(from: ID, to: ID) =>
    path <- N<Station>(from)::ShortestPathBFS<BusRoute>::To(to)
RETURN path

QUERY busDijkstra(from: ID, to: ID) =>
            path <- N<Station>(from)::ShortestPathDijkstras<BusRoute>(_::{duration_minutes})::To(to)
RETURN path

// ─── Complex Path Queries ────────────────────────────────────────

// Multiple shortest paths in one query
QUERY compareAlgorithms(start: ID, end: ID) =>
    bfs_path <- N<Location>(start)::ShortestPathBFS<Route>::To(end)
    dijkstra_path <- N<Location>(start)::ShortestPathDijkstras<Route>(_::{distance})::To(end)
RETURN {
    bfs: bfs_path,
    dijkstra: dijkstra_path
}

// Path with intermediate constraints
QUERY routeWithConstraints(start: ID, end: ID) =>
    locations <- N<Location>(start)::ShortestPathBFS<Route>::To(end)
RETURN locations

// ─── Edge Cases and Error Handling ───────────────────────────────

// Non-existent path
QUERY noPath(start: ID, end: ID) =>
    path <- N<Location>(start)::ShortestPathDijkstras<Route>(_::{distance})::To(end)
RETURN path

// Self-loop (start equals end)
QUERY selfPath(node: ID) =>
    path <- N<Location>(node)::ShortestPathBFS<Route>::To(node)
RETURN path

// Bidirectional paths
QUERY bidirectionalPaths(a: ID, b: ID) =>
    forward_bfs <- N<Location>(a)::ShortestPathBFS<Route>::To(b)
    backward_bfs <- N<Location>(b)::ShortestPathBFS<Route>::From(a)
    forward_dijkstra <- N<Location>(a)::ShortestPathDijkstras<Route>(_::{distance})::To(b)
    backward_dijkstra <- N<Location>(b)::ShortestPathDijkstras<Route>(_::{distance})::From(a)
RETURN {
    forward_bfs: forward_bfs,
    backward_bfs: backward_bfs,
    forward_dijkstra: forward_dijkstra,
    backward_dijkstra: backward_dijkstra
}