// Shortest path documentation examples

// BFS finds path with minimum hops (default behavior)
QUERY GetShortestPathBFS (from_id: ID, to_id: ID) =>
    path <- N<City>(from_id)::ShortestPath<Road>::To(to_id)
    RETURN path

// Explicit BFS (same as above)
QUERY GetShortestPathBFSExplicit (from_id: ID, to_id: ID) =>
    path <- N<City>(from_id)::ShortestPathBFS<Road>::To(to_id)
    RETURN path

// Dijkstra finds path with minimum total distance
QUERY GetShortestPathDijkstra (from_id: ID, to_id: ID) =>
    path <- N<City>(from_id)::ShortestPathDijkstras<Road>(_::{distance_km})::To(to_id)
    RETURN path

// Location-based shortest path (Example 2)
QUERY GetShortestPath (from_id: ID, to_id: ID) =>
    path <- N<Location>(from_id)::ShortestPath<LocationRoad>::To(to_id)
    RETURN path

// Helper queries
QUERY CreateCity (name: String) =>
    city <- AddN<City>({name: name})
    RETURN city

QUERY ConnectCities (from_id: ID, to_id: ID, distance_km: F64) =>
    road <- AddE<Road>({
        distance_km: distance_km,
    })::From(from_id)::To(to_id)
    RETURN road

QUERY CreateLocation (name: String) =>
    location <- AddN<Location>({
        name: name,
    })
    RETURN location

QUERY ConnectLocations (from_id: ID, to_id: ID, distance_km: U32) =>
    road <- AddE<LocationRoad>({
        distance_km: distance_km,
    })::From(from_id)::To(to_id)
    RETURN road
