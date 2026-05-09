//! Memory lifecycle management - tiered Working / ShortTerm / LongTerm flow.
//!
//! Port of `memst-core/src/memory/mod.rs`. The lifecycle controls how memories
//! transition between tiers, when they get compacted, and how much token budget
//! each tier may consume. This is the *different levels of memory management*
//! capability requested when integrating MemSt into HelixDB.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::error::{Error, Result};
use super::objects::{MemoryScope, ObjectId};
use super::types::{MemoryItem, MemoryTier};

/// Memory lifecycle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemoryState {
    /// Active in working memory
    Active,
    /// Marked for promotion
    PendingPromotion,
    /// Currently being compacted
    Compacting,
    /// In short-term storage
    ShortTerm,
    /// In long-term storage
    LongTerm,
    /// Archived (not in context)
    Archived,
    /// Tombstoned (deleted but preserved for history)
    Tombstoned,
}

/// Trigger that caused a lifecycle transition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransitionTrigger {
    /// Token budget exceeded
    TokenBudgetExceeded,
    /// TTL expired
    TtlExpired,
    /// Access count threshold
    AccessCount(u32),
    /// Importance threshold crossed
    ImportanceThreshold(f32),
    /// Manually triggered
    Manual,
    /// Sleep-time consolidation
    SleepConsolidation,
    /// Conflict detected
    ConflictDetected,
}

/// Record of a single state transition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transition {
    /// Origin state
    pub from: MemoryState,
    /// Destination state
    pub to: MemoryState,
    /// Cause
    pub trigger: TransitionTrigger,
    /// When the transition happened
    pub timestamp: DateTime<Utc>,
    /// Commit hash (if persisted)
    pub commit_hash: Option<ObjectId>,
    /// Token delta induced by the transition
    pub token_delta: i32,
}

/// Lossless rewind point captured before a compaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionCheckpoint {
    /// Checkpoint id
    pub id: String,
    /// When it was created
    pub timestamp: DateTime<Utc>,
    /// Optional scope of the checkpoint
    pub scope: Option<MemoryScope>,
    /// Source tier
    pub source_tier: MemoryTier,
    /// Destination tier
    pub target_tier: MemoryTier,
    /// Memory ids included
    pub memory_ids: Vec<String>,
    /// Commit hash before compaction
    pub pre_commit: ObjectId,
    /// Commit hash after compaction (set on finalize)
    pub post_commit: Option<ObjectId>,
    /// Original token count
    pub original_tokens: u32,
    /// Compacted token count
    pub compacted_tokens: u32,
}

/// Configuration for [`MemoryLifecycle`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleConfig {
    /// Maximum tokens in working memory
    pub working_memory_max_tokens: u32,
    /// Maximum tokens in short-term memory
    pub short_term_max_tokens: u32,
    /// Maximum tokens in long-term memory
    pub long_term_max_tokens: u32,
    /// Working-memory TTL in seconds
    pub working_memory_ttl: u64,
    /// Short-term TTL in seconds
    pub short_term_ttl: u64,
    /// Access count threshold for promotion
    pub promotion_access_threshold: u32,
    /// Importance threshold for long-term storage
    pub long_term_importance_threshold: f32,
    /// Whether to compact automatically when budgets are exceeded
    pub auto_compact: bool,
    /// Compaction batch size
    pub compaction_batch_size: usize,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            working_memory_max_tokens: 4096,
            short_term_max_tokens: 16384,
            long_term_max_tokens: 1_048_576,
            working_memory_ttl: 3_600,
            short_term_ttl: 86_400,
            promotion_access_threshold: 3,
            long_term_importance_threshold: 0.7,
            auto_compact: true,
            compaction_batch_size: 100,
        }
    }
}

/// Memory lifecycle manager.
///
/// Tracks the current [`MemoryState`] of each memory id, records every
/// transition, and exposes helpers for selecting candidates for promotion or
/// compaction.
pub struct MemoryLifecycle {
    config: LifecycleConfig,
    states: HashMap<String, MemoryState>,
    transitions: Vec<Transition>,
    checkpoints: Vec<CompactionCheckpoint>,
}

impl MemoryLifecycle {
    /// Create a new lifecycle manager.
    pub fn new(config: LifecycleConfig) -> Self {
        Self {
            config,
            states: HashMap::new(),
            transitions: Vec::new(),
            checkpoints: Vec::new(),
        }
    }

    /// Default lifecycle configuration.
    pub fn default_config() -> LifecycleConfig {
        LifecycleConfig::default()
    }

    /// Register a memory with an initial tier and return its computed state.
    pub fn register(&mut self, memory_id: &str, initial_tier: MemoryTier) -> MemoryState {
        let state = match initial_tier {
            MemoryTier::Working => MemoryState::Active,
            MemoryTier::ShortTerm => MemoryState::ShortTerm,
            MemoryTier::LongTerm => MemoryState::LongTerm,
        };
        self.states.insert(memory_id.to_string(), state);
        state
    }

    /// Look up the current state of a memory.
    pub fn get_state(&self, memory_id: &str) -> Option<MemoryState> {
        self.states.get(memory_id).copied()
    }

    /// Decide if a memory should be promoted, returning the trigger if so.
    pub fn should_promote(&self, item: &MemoryItem) -> Option<TransitionTrigger> {
        if item.access_count >= self.config.promotion_access_threshold {
            return Some(TransitionTrigger::AccessCount(item.access_count));
        }
        if item.importance >= self.config.long_term_importance_threshold {
            return Some(TransitionTrigger::ImportanceThreshold(item.importance));
        }
        None
    }

    /// Decide if a memory should be demoted/archived from `current_tier`.
    pub fn should_demote(
        &self,
        item: &MemoryItem,
        current_tier: MemoryTier,
    ) -> Option<TransitionTrigger> {
        let age = Utc::now() - item.last_accessed;
        match current_tier {
            MemoryTier::Working => {
                if age > Duration::seconds(self.config.working_memory_ttl as i64) {
                    Some(TransitionTrigger::TtlExpired)
                } else {
                    None
                }
            }
            MemoryTier::ShortTerm => {
                if age > Duration::seconds(self.config.short_term_ttl as i64) {
                    Some(TransitionTrigger::TtlExpired)
                } else {
                    None
                }
            }
            MemoryTier::LongTerm => None,
        }
    }

    /// Apply a state transition and append it to the history.
    pub fn transition(
        &mut self,
        memory_id: &str,
        to_state: MemoryState,
        trigger: TransitionTrigger,
        token_delta: i32,
    ) -> Result<Transition> {
        let from_state = self
            .states
            .get(memory_id)
            .copied()
            .unwrap_or(MemoryState::Active);

        let transition = Transition {
            from: from_state,
            to: to_state,
            trigger,
            timestamp: Utc::now(),
            commit_hash: None,
            token_delta,
        };

        self.states.insert(memory_id.to_string(), to_state);
        self.transitions.push(transition.clone());
        Ok(transition)
    }

    /// Open a compaction checkpoint.
    pub fn create_checkpoint(
        &mut self,
        scope: Option<MemoryScope>,
        source_tier: MemoryTier,
        target_tier: MemoryTier,
        memory_ids: Vec<String>,
        pre_commit: ObjectId,
        original_tokens: u32,
    ) -> CompactionCheckpoint {
        let checkpoint = CompactionCheckpoint {
            id: format!(
                "chk-{}-{}",
                pre_commit.abbreviate(),
                Utc::now().timestamp_millis()
            ),
            timestamp: Utc::now(),
            scope,
            source_tier,
            target_tier,
            memory_ids,
            pre_commit,
            post_commit: None,
            original_tokens,
            compacted_tokens: 0,
        };
        self.checkpoints.push(checkpoint.clone());
        checkpoint
    }

    /// Finalize a checkpoint with the resulting commit and token count.
    pub fn finalize_checkpoint(
        &mut self,
        checkpoint_id: &str,
        post_commit: ObjectId,
        compacted_tokens: u32,
    ) -> Result<()> {
        if let Some(cp) = self
            .checkpoints
            .iter_mut()
            .find(|c| c.id == checkpoint_id)
        {
            cp.post_commit = Some(post_commit);
            cp.compacted_tokens = compacted_tokens;
            Ok(())
        } else {
            Err(Error::InvalidOperation(format!(
                "Checkpoint {} not found",
                checkpoint_id
            )))
        }
    }

    /// Look up a checkpoint by id (used for lossless rewind).
    pub fn restore_checkpoint(&self, checkpoint_id: &str) -> Option<&CompactionCheckpoint> {
        self.checkpoints.iter().find(|c| c.id == checkpoint_id)
    }

    /// Slice of all checkpoints.
    pub fn list_checkpoints(&self) -> &[CompactionCheckpoint] {
        &self.checkpoints
    }

    /// Slice of every transition recorded so far.
    pub fn transitions(&self) -> &[Transition] {
        &self.transitions
    }

    /// Sum of `token_estimate` per tier across the supplied memories.
    pub fn calculate_token_usage(&self, memories: &[MemoryItem]) -> HashMap<MemoryTier, u32> {
        let mut usage: HashMap<MemoryTier, u32> = HashMap::new();

        for item in memories {
            if let Some(state) = self.get_state(&item.id.to_string()) {
                let tier = match state {
                    MemoryState::Active
                    | MemoryState::PendingPromotion
                    | MemoryState::Compacting => MemoryTier::Working,
                    MemoryState::ShortTerm => MemoryTier::ShortTerm,
                    MemoryState::LongTerm
                    | MemoryState::Archived
                    | MemoryState::Tombstoned => MemoryTier::LongTerm,
                };

                *usage.entry(tier).or_insert(0) += item.token_estimate.unwrap_or(0);
            }
        }
        usage
    }

    /// Whether `tier` is over its budget for the supplied memories.
    pub fn is_budget_exceeded(&self, memories: &[MemoryItem], tier: MemoryTier) -> bool {
        let usage = self.calculate_token_usage(memories);
        let current = usage.get(&tier).copied().unwrap_or(0);
        let limit = match tier {
            MemoryTier::Working => self.config.working_memory_max_tokens,
            MemoryTier::ShortTerm => self.config.short_term_max_tokens,
            MemoryTier::LongTerm => self.config.long_term_max_tokens,
        };
        current > limit
    }

    /// Select up to `max_count` memories from `source_tier` for compaction,
    /// preferring lower importance and older `last_accessed` first.
    pub fn select_for_compaction<'a>(
        &self,
        memories: &'a [MemoryItem],
        source_tier: MemoryTier,
        max_count: usize,
    ) -> Vec<&'a MemoryItem> {
        let mut candidates: Vec<&'a MemoryItem> = memories
            .iter()
            .filter(|m| {
                let state = self.get_state(&m.id.to_string());
                match source_tier {
                    MemoryTier::Working => {
                        state == Some(MemoryState::Active)
                            || state == Some(MemoryState::PendingPromotion)
                    }
                    MemoryTier::ShortTerm => state == Some(MemoryState::ShortTerm),
                    MemoryTier::LongTerm => state == Some(MemoryState::LongTerm),
                }
            })
            .collect();

        candidates.sort_by(|a, b| {
            let importance_cmp = a
                .importance
                .partial_cmp(&b.importance)
                .unwrap_or(std::cmp::Ordering::Equal);
            if importance_cmp == std::cmp::Ordering::Equal {
                a.last_accessed.cmp(&b.last_accessed)
            } else {
                importance_cmp
            }
        });

        candidates.into_iter().take(max_count).collect()
    }
}

/// Pluggable token counter interface.
pub trait TokenCounter: Send + Sync {
    /// Count tokens in raw text.
    fn count(&self, text: &str) -> u32;

    /// Count tokens in a memory item (default: counts `item.content`).
    fn count_memory(&self, item: &MemoryItem) -> u32 {
        self.count(&item.content)
    }
}

/// Whitespace-based token counter (fast approximation).
pub struct SimpleTokenCounter;

impl TokenCounter for SimpleTokenCounter {
    fn count(&self, text: &str) -> u32 {
        text.split_whitespace().count() as u32
    }
}

/// Configuration-based token counter (English-tuned by default).
pub struct ConfiguredTokenCounter {
    tokens_per_word: f32,
    overhead: u32,
}

impl ConfiguredTokenCounter {
    /// Default English-tuned counter.
    pub fn new() -> Self {
        Self {
            tokens_per_word: 1.3,
            overhead: 3,
        }
    }

    /// Customize the words->tokens ratio and per-message overhead.
    pub fn with_ratio(tokens_per_word: f32, overhead: u32) -> Self {
        Self {
            tokens_per_word,
            overhead,
        }
    }
}

impl Default for ConfiguredTokenCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl TokenCounter for ConfiguredTokenCounter {
    fn count(&self, text: &str) -> u32 {
        let word_count = text.split_whitespace().count() as f32;
        (word_count * self.tokens_per_word) as u32 + self.overhead
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mem(content: &str, importance: f32) -> MemoryItem {
        MemoryItem::new(content, "test").with_confidence(importance)
    }

    #[test]
    fn lifecycle_register_and_state() {
        let mut lifecycle = MemoryLifecycle::new(LifecycleConfig::default());
        let s = lifecycle.register("m1", MemoryTier::Working);
        assert_eq!(s, MemoryState::Active);
        assert_eq!(lifecycle.get_state("m1"), Some(MemoryState::Active));
    }

    #[test]
    fn promotion_triggers_on_access_count() {
        let lifecycle = MemoryLifecycle::new(LifecycleConfig::default());
        let mut item = mem("x", 0.5);
        item.access_count = 5;
        assert!(lifecycle.should_promote(&item).is_some());
    }

    #[test]
    fn demotion_triggers_on_ttl() {
        let lifecycle = MemoryLifecycle::new(LifecycleConfig::default());
        let mut item = mem("x", 0.5);
        item.last_accessed = Utc::now() - Duration::hours(2);
        assert!(lifecycle.should_demote(&item, MemoryTier::Working).is_some());
    }

    #[test]
    fn transition_history_recorded() {
        let mut lifecycle = MemoryLifecycle::new(LifecycleConfig::default());
        lifecycle.register("m1", MemoryTier::Working);
        let t = lifecycle
            .transition("m1", MemoryState::ShortTerm, TransitionTrigger::TtlExpired, -100)
            .unwrap();
        assert_eq!(t.from, MemoryState::Active);
        assert_eq!(t.to, MemoryState::ShortTerm);
        assert_eq!(lifecycle.transitions().len(), 1);
    }

    #[test]
    fn checkpoint_create_and_restore() {
        let mut lifecycle = MemoryLifecycle::new(LifecycleConfig::default());
        let pre = ObjectId::from_content(b"x");
        let cp = lifecycle.create_checkpoint(
            None,
            MemoryTier::Working,
            MemoryTier::ShortTerm,
            vec!["m1".into(), "m2".into()],
            pre,
            500,
        );
        assert!(cp.id.starts_with("chk-"));
        assert!(lifecycle.restore_checkpoint(&cp.id).is_some());
    }

    #[test]
    fn token_counter_estimates() {
        let counter = ConfiguredTokenCounter::new();
        let n = counter.count("hello world this is a test");
        assert!(n > 6 && n < 20);
    }
}
