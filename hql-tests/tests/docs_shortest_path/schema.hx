// Shortest path documentation examples

N::City {
    name: String,
}

N::Location {
    name: String,
}

E::Road {
    From: City,
    To: City,
    Properties: {
        distance_km: F64,
    }
}

E::LocationRoad {
    From: Location,
    To: Location,
    Properties: {
        distance_km: U32,
    }
}
