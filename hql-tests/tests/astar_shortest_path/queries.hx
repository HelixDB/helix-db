// ============================================================================
// A* SHORTEST PATH TESTS
// ============================================================================

// Test 1: Basic A* with default weight and property-based heuristic
QUERY test_astar_basic(start: ID, end: ID) =>
    result <- N<City>(start)
    ::ShortestPathAStar<Road>(_::{distance}, "h")
    ::To(end)
    RETURN result

// Test 2: A* with custom weight formula and heuristic
QUERY test_astar_custom_weight(start: ID, end: ID) =>
    // Weight = distance * traffic_factor
    result <- N<City>(start)
    ::ShortestPathAStar<Road>(MUL(_::{distance}, _::{traffic_factor}), "h")
    ::To(end)
    RETURN result

// Test 3: A* with complex weight expression
QUERY test_astar_complex_weight(start: ID, end: ID) =>
    // Weight = distance * (1 + traffic_factor/10)
    result <- N<City>(start)
    ::ShortestPathAStar<Road>(
        MUL(_::{distance}, ADD(1, DIV(_::{traffic_factor}, 10))),
        "h"
    )
    ::To(end)
    RETURN result

// Test 4: A* with source node context
QUERY test_astar_with_source_context(start: ID, end: ID) =>
    // Weight = distance * source_traffic_factor
    result <- N<City>(start)
    ::ShortestPathAStar<Road>(
        MUL(_::{distance}, _::FromN::{traffic_factor}),
        "h"
    )
    ::To(end)
    RETURN result

// Test 5: A* finding optimal path
// This should find start->mid1->goal (cost 10) instead of start->mid2->goal (cost 16)
QUERY test_astar_optimal_path(start: ID, end: ID) =>
    result <- N<City>(start)
    ::ShortestPathAStar<Road>(_::{distance}, "h")
    ::To(end)
    RETURN result
