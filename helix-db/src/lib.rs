pub mod helix_engine;
pub mod helix_gateway;
#[cfg(feature = "compiler")]
pub mod helixc;
pub mod protocol;
pub mod utils;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;