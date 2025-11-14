N::Location {
    name: String,
    traffic_factor: F64,
    popularity: F64
}

E::Route {
    From: Location,
    To: Location,
    Properties: {
        distance: F64,
        days_since_update: F64,
        bandwidth: F64,
        reliability: F64
    }
}
