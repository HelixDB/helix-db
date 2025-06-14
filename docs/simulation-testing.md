# Deterministic Simulation Testing

This document outlines the design for a deterministic simulation testing engine inspired by TigerBeetle's VOPR. The goal is to run HelixDB in a fully controlled environment where network, storage and asynchronous execution are scheduled deterministically.

## Motivation
Deterministic simulation allows us to reproduce race conditions, inject faults and run large workloads at accelerated speed. By running real code under a custom scheduler we can explore many failure scenarios in CI.

## Architecture Overview
1. **Async Runtime Abstraction**
   - Introduce an `AsyncRuntime` trait providing primitives such as `spawn` and `sleep`.
   - Production uses a Tokio backed implementation.
   - A deterministic scheduler will implement the same trait for testing, controlling the order of task execution.

2. **Transport Abstraction**
   - Extract a `Transport` trait that wraps accepting and connecting TCP streams.
   - `ConnectionHandler` and the thread pool depend on this trait instead of `TcpListener`/`TcpStream` directly.
   - The simulator implements in-memory transport that can delay, drop or reorder packets.

3. **Storage Abstraction**
   - Define a `Storage` trait around LMDB operations in `helix_engine`.
   - Implementations include the current LMDB backend and a simulated store capable of injecting errors or latency.

4. **Simulation Harness**
   - Compose the deterministic runtime, simulated transport and storage.
   - Run one or more HelixDB instances inside the same process to model a cluster.
   - Drive workloads, schedule events and collect metrics for fuzzing and invariant checking.


