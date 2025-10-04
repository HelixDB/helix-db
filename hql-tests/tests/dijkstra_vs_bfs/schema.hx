// Schema for testing Dijkstra vs BFS path algorithms
// This schema creates a graph where BFS and Dijkstra will find different paths

N::Location {
    INDEX name: String,
    description: String,
}

E::Route {
    From: Location,
    To: Location,
    Properties: {
        distance: F64,      // Distance in kilometers (for Dijkstra)
        toll_cost: F64,     // Additional cost factor
        scenic_rating: I32, // 1-10 rating for scenic beauty
    }
}

// Alternative edge type for testing different weight attributes
E::FlightPath {
    From: Location,
    To: Location,
    Properties: {
        flight_time: F64,   // Time in hours
        cost: F64,          // Cost in dollars
        airline: String,    // Airline name
    }
}

// Social network example to test BFS on unweighted graphs
N::Person {
    INDEX username: String,
    name: String,
    age: I32,
}

E::Follows {
    From: Person,
    To: Person,
    Properties: {
        since_year: I32,
        interaction_score: F64, // How much they interact (0-1)
    }
}

// Transportation network with multiple types of connections
N::Station {
    INDEX code: String,
    name: String,
    city: String,
}

E::TrainRoute {
    From: Station,
    To: Station,
    Properties: {
        duration_minutes: I32,
        price: F64,
        high_speed: String,
    }
}

E::BusRoute {
    From: Station,
    To: Station,
    Properties: {
        duration_minutes: I32,
        price: F64,
        stops: I32,        // Number of stops
    }
}