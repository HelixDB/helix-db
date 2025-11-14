N::City {
    name: String,
    h: F64
}

E::Road {
    From: City,
    To: City,
    Properties: {
        distance: F64,
        traffic_factor: F64
    }
}
