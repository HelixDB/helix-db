fn main() {
    tonic_prost_build::configure()
        .bytes(".helixdb.server.v1.QueryJsonRequest.body")
        .bytes(".helixdb.server.v1.QueryJsonResponse.body")
        .compile_protos(&["proto/helixdb.proto"], &["proto"])
        .expect("compile helix db server protobuf definitions");
}
