// Minimal build.rs using helix-lib's compile_queries_default()
fn main() {
    helix_lib::build::compile_queries_default().expect("Failed to compile Helix queries");
}
