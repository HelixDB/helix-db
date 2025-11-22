# helix-sim: Minimal Deterministic Runtime

A minimal deterministic async runtime for HelixDB simulation and testing.

## Features

- **TokioRuntime**: Adapter for production tokio runtime
- **DeterministicRuntime**: Deterministic runtime with virtual time for testing
- **SleepFuture**: Unified sleep interface for both runtimes

## AsyncRuntime Trait

```rust
pub trait AsyncRuntime {
    fn spawn_boxed(&self, fut: Pin<Box<dyn Future<Output = ()> + Send>>) -> Box<dyn std::any::Any>;
    fn sleep(&self, dur: Duration) -> SleepFuture;
    fn now(&self) -> u64;
}
```

## Usage

### Production (Tokio)

```rust
let runtime = TokioRuntime;
let sleep = runtime.sleep(Duration::from_secs(1));
```

### Testing (Deterministic)

```rust
let runtime = DeterministicRuntime::new();

// Virtual time starts at 0
assert_eq!(runtime.now(), 0);

// Advance virtual time
runtime.advance_to(1000);
assert_eq!(runtime.now(), 1000);

// Sleep futures respond to virtual time
let sleep = runtime.sleep(Duration::from_nanos(500));
// Will complete when advanced_to >= wake_time
```

## Integration

MetricsSender in helix-cli now uses TokioRuntime, allowing it to be swapped for DeterministicRuntime in tests.
