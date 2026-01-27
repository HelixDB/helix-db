N::Test {
    INDEX testfield: String,
}

E::TestEdge {
    From: Test,
    To: Test,
    Properties: { 
        since: I32,
    }
}
