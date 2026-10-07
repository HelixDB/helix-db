//! A pool of reset arenas, so steady-state requests parse without asking the
//! system allocator for any arena memory at all.

use std::num::NonZeroUsize;
use std::sync::Mutex;

use super::Bump;

/// How a [`Pool`] sizes, limits and keeps its arenas.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolConfig {
    /// Capacity of an arena created when the pool has none idle.
    pub initial_chunk_bytes: usize,
    /// An arena whose chunks grew past this is freed on return rather than
    /// kept, so one oversized request cannot hold its memory forever.
    pub retain_bytes: usize,
    /// Most arenas kept idle; more returning at once are freed.
    pub max_idle: usize,
    /// Per-request cap on arena memory, enforced by every allocation; `None`
    /// leaves arenas unbounded. The first chunk counts against it: when
    /// `initial_chunk_bytes` does not fit, a new arena starts empty and sizes
    /// its chunks under the limit.
    pub allocation_limit: Option<NonZeroUsize>,
}

/// Idle arenas shared by concurrent requests.
///
/// ```
/// use helix_ast::arena::{Pool, PoolConfig};
///
/// let pool = Pool::new(PoolConfig {
///     initial_chunk_bytes: 4096,
///     retain_bytes: 1 << 20,
///     max_idle: 8,
///     allocation_limit: None,
/// });
/// let first = {
///     let bump = pool.checkout();
///     bump.alloc_str("request").as_ptr() as usize
/// };
/// // The returned arena was reset and is reused, chunk and all.
/// let bump = pool.checkout();
/// assert_eq!(bump.alloc_str("request").as_ptr() as usize, first);
/// assert_eq!(pool.idle(), 0);
/// ```
pub struct Pool {
    idle: Mutex<Vec<Bump>>,
    config: PoolConfig,
}

impl Pool {
    /// An empty pool; arenas are created on demand.
    pub const fn new(config: PoolConfig) -> Self {
        Self {
            idle: Mutex::new(Vec::new()),
            config,
        }
    }

    /// An idle arena, or a new one when none is idle. It returns to the pool
    /// when dropped.
    pub fn checkout(&self) -> PooledBump<'_> {
        let idle = self
            .idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop();
        let bump = idle.unwrap_or_else(|| {
            let bump = Bump::with_capacity(self.config.initial_chunk_bytes);
            // bumpalo checks the limit only when it adds a chunk, and rounds
            // chunk sizes up, so an oversized first chunk is checked here.
            match self.config.allocation_limit {
                Some(limit) if bump.allocated_bytes() > limit.get() => Bump::new(),
                _ => bump,
            }
        });
        bump.set_allocation_limit(self.config.allocation_limit.map(NonZeroUsize::get));
        PooledBump { bump, pool: self }
    }

    /// Number of idle arenas.
    pub fn idle(&self) -> usize {
        self.idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

/// An arena checked out of a [`Pool`]. It is `Send`, so a request may own it
/// across awaits, and dereferences to its [`Bump`].
pub struct PooledBump<'p> {
    bump: Bump,
    pool: &'p Pool,
}

impl std::ops::Deref for PooledBump<'_> {
    type Target = Bump;

    fn deref(&self) -> &Bump {
        &self.bump
    }
}

impl Drop for PooledBump<'_> {
    fn drop(&mut self) {
        // `Bump::new` does not allocate, so taking the arena out is free.
        let mut bump = std::mem::take(&mut self.bump);
        if bump.allocated_bytes() > self.pool.config.retain_bytes {
            return;
        }
        // Reset outside the lock: it frees every chunk but the largest.
        bump.reset();
        let mut idle = self
            .pool
            .idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if idle.len() < self.pool.config.max_idle {
            idle.push(bump);
        }
    }
}
