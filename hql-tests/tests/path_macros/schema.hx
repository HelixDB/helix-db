// Schema for path macro tests

N::City {
    name: String,
}

E::Road {
    From: City,
    To: City,
    Properties: {
        weight: F64,
    }
}