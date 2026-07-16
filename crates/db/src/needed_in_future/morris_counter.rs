//! Morris Counter - probabilistic counter for approximate degree counting
//!
//! Uses logarithmic space (1 byte per node) to approximately count degrees.
//! Useful for identifying high-degree vertices without exact counting overhead.

use std::collections::HashMap;

use parking_lot::RwLock;
use rand::Rng;

use crate::graph::NodeId;

/// Morris Counter - probabilistic counter for degree approximation
///
/// Uses 1 byte per node to approximately track degree counts.
/// The counter uses a floating-point representation with 3 exponent bits
/// and 5 mantissa bits, allowing it to represent values up to ~8 million
/// with bounded relative error.
///
/// # Thread Safety
///
/// This implementation is thread-safe and can be shared across threads.
///
/// # Example
///
/// ```ignore
/// use helix::filters::MorrisCounter;
///
/// let counter = MorrisCounter::new();
///
/// // Probabilistically increment counter for node 42
/// counter.add(42);
/// counter.add(42);
/// counter.add(42);
///
/// // Get approximate count
/// let approx_degree = counter.get(42);
/// ```
pub struct MorrisCounter {
    /// HashMap of node ID to 8-bit counter (sparse storage)
    counters: RwLock<HashMap<NodeId, u8>>,
    /// Number of exponent bits (default 3)
    exponent_bits: i32,
    /// Number of mantissa bits (default 5)
    mantissa_bits: i32,
}

impl MorrisCounter {
    /// Create a new MorrisCounter
    pub fn new() -> Self {
        Self {
            counters: RwLock::new(HashMap::new()),
            exponent_bits: 3,
            mantissa_bits: 5,
        }
    }

    /// Create a new MorrisCounter with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            counters: RwLock::new(HashMap::with_capacity(capacity)),
            exponent_bits: 3,
            mantissa_bits: 5,
        }
    }

    /// Probabilistically increment the counter for a node
    ///
    /// As the count grows, increments become probabilistically less likely,
    /// maintaining approximate counts with bounded relative error.
    pub fn add(&self, node: NodeId) {
        let mut counters = self.counters.write();
        let counter = counters.entry(node).or_insert(0);

        // Don't increment if already at max
        if *counter == u8::MAX {
            return;
        }

        let exponent = self.extract_exponent(*counter);
        let prob = 1u32 << exponent; // 2^exponent

        // Probabilistically increment (1/prob chance)
        let mut rng = rand::rng();
        if rng.random_range(1..=prob) == 1 {
            *counter = counter.wrapping_add(1);
        }
    }

    /// Probabilistically decrement the counter for a node
    ///
    /// Used when edges are removed. Maintains the probabilistic properties.
    pub fn decay(&self, node: NodeId) {
        let mut counters = self.counters.write();

        let Some(counter) = counters.get_mut(&node) else {
            return;
        };

        if *counter == 0 {
            return;
        }

        let exponent = self.extract_exponent(*counter);
        let prob = 1u32 << exponent; // 2^exponent

        // Probabilistically decrement (1/prob chance)
        let mut rng = rand::rng();
        if rng.random_range(1..=prob) == 1 {
            *counter = counter.wrapping_sub(1);
        }
    }

    /// Get the approximate count for a node
    ///
    /// Returns an approximate degree count. The relative error is bounded
    /// by the number of mantissa bits.
    pub fn get(&self, node: NodeId) -> u64 {
        let counters = self.counters.read();

        let Some(&counter) = counters.get(&node) else {
            return 0;
        };

        if counter == u8::MAX {
            return u64::MAX;
        }

        let exponent = self.extract_exponent(counter);
        let mantissa = self.extract_mantissa(counter);

        // Formula: (2^exponent - 1) * 2^mantissa_bits + 2^exponent * mantissa
        let exp_val = 1u64 << exponent;
        let mant_bits = 1u64 << self.mantissa_bits;

        (exp_val - 1) * mant_bits + exp_val * mantissa as u64
    }

    /// Check if a node appears to be high-degree (above threshold)
    ///
    /// Useful for quickly filtering vertices during traversal.
    pub fn is_high_degree(&self, node: NodeId, threshold: u64) -> bool {
        self.get(node) >= threshold
    }

    /// Get memory usage in bytes (approximate)
    pub fn memory_usage(&self) -> usize {
        let counters = self.counters.read();
        // Each entry: NodeId (8 bytes) + u8 (1 byte) + HashMap overhead (~24 bytes)
        counters.len() * 33 + std::mem::size_of::<HashMap<NodeId, u8>>()
    }

    /// Get the number of tracked vertices
    pub fn len(&self) -> usize {
        self.counters.read().len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.counters.read().is_empty()
    }

    /// Clear all counters
    pub fn clear(&self) {
        self.counters.write().clear();
    }

    /// Extract exponent bits from counter value (top 3 bits)
    #[inline]
    fn extract_exponent(&self, counter: u8) -> i32 {
        let mask = ((1u8 << self.exponent_bits) - 1) << (8 - self.exponent_bits);
        ((counter & mask) >> (8 - self.exponent_bits)) as i32
    }

    /// Extract mantissa bits from counter value (bottom 5 bits)
    #[inline]
    fn extract_mantissa(&self, counter: u8) -> i32 {
        let mask = (1u8 << self.mantissa_bits) - 1;
        (counter & mask) as i32
    }
}

impl Default for MorrisCounter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_counter() {
        let counter = MorrisCounter::new();
        assert!(counter.is_empty());
    }

    #[test]
    fn test_with_capacity() {
        let counter = MorrisCounter::with_capacity(100);
        assert!(counter.is_empty());
    }

    #[test]
    fn test_get_zero() {
        let counter = MorrisCounter::new();
        assert_eq!(counter.get(0), 0);
        assert_eq!(counter.get(5), 0);
        assert_eq!(counter.get(u64::MAX), 0);
    }

    #[test]
    fn test_add_creates_entry() {
        let counter = MorrisCounter::new();
        counter.add(10);
        assert_eq!(counter.len(), 1);
    }

    #[test]
    fn test_large_ids() {
        let counter = MorrisCounter::new();

        // Should handle large IDs without memory issues
        counter.add(u64::MAX - 1);
        counter.add(1_000_000_000);

        assert_eq!(counter.len(), 2);
    }

    #[test]
    fn test_extract_exponent() {
        let counter = MorrisCounter::new();
        // With 3 exponent bits at the top:
        // 0b11100000 (224) should have exponent 7
        // 0b00100000 (32) should have exponent 1
        assert_eq!(counter.extract_exponent(0b11100000), 7);
        assert_eq!(counter.extract_exponent(0b00100000), 1);
        assert_eq!(counter.extract_exponent(0b00000000), 0);
    }

    #[test]
    fn test_extract_mantissa() {
        let counter = MorrisCounter::new();
        // With 5 mantissa bits at the bottom:
        // 0b00011111 (31) should have mantissa 31
        // 0b00010101 (21) should have mantissa 21
        assert_eq!(counter.extract_mantissa(0b00011111), 31);
        assert_eq!(counter.extract_mantissa(0b00010101), 21);
        assert_eq!(counter.extract_mantissa(0b11100000), 0);
    }

    #[test]
    fn test_add_probabilistic() {
        let counter = MorrisCounter::new();

        // Add many times, should eventually increment
        for _ in 0..1000 {
            counter.add(0);
        }

        // Should have some count after many increments
        // (probabilistic, but very unlikely to be 0)
        assert!(counter.get(0) > 0);
    }

    #[test]
    fn test_is_high_degree() {
        let counter = MorrisCounter::new();

        // Add many times to build up count
        for _ in 0..10000 {
            counter.add(5);
        }

        // Should be considered high degree
        assert!(counter.is_high_degree(5, 100));
        // Untouched node should not be high degree
        assert!(!counter.is_high_degree(1, 100));
    }

    #[test]
    fn test_decay() {
        let counter = MorrisCounter::new();

        // Build up a count first
        for _ in 0..10000 {
            counter.add(0);
        }
        let initial = counter.get(0);

        // Decay many times
        for _ in 0..5000 {
            counter.decay(0);
        }

        // Count should have decreased (probabilistic)
        // Note: due to probabilistic nature, we just check it's not higher
        assert!(counter.get(0) <= initial);
    }

    #[test]
    fn test_clear() {
        let counter = MorrisCounter::new();
        for _ in 0..1000 {
            counter.add(0);
        }
        assert!(counter.get(0) > 0);
        assert!(!counter.is_empty());

        counter.clear();
        assert_eq!(counter.get(0), 0);
        assert!(counter.is_empty());
    }

    #[test]
    fn test_memory_usage() {
        let counter = MorrisCounter::new();
        for i in 0..100u64 {
            counter.add(i);
        }
        assert!(counter.memory_usage() > 0);
    }

    #[test]
    fn test_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let counter = Arc::new(MorrisCounter::new());
        let mut handles = vec![];

        // Spawn multiple threads adding to the same counter
        for _ in 0..4 {
            let counter = Arc::clone(&counter);
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    counter.add(0);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Should have accumulated some count
        assert!(counter.get(0) > 0);
    }
}
