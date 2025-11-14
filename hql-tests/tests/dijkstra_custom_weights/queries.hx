// ============================================================================
// EXPONENTIAL DECAY WEIGHT TESTS
// ============================================================================

QUERY test_exponential_decay_weight(start: ID, end: ID) =>
    // Weight calculation: 0.95^(days_since_update / 30)
    // This gives lower weight to fresher routes
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(POW(0.95, DIV(_::{days_since_update}, 30)))
    ::To(end)
    RETURN result

QUERY test_simple_property_weight(start: ID, end: ID) =>
    // Simple property-based weight (backward compatible)
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(_::{distance})
    ::To(end)
    RETURN result

// ============================================================================
// MULTI-FACTOR WEIGHTS
// ============================================================================

QUERY test_distance_with_traffic(start: ID, end: ID) =>
    // Weight = distance * source_traffic_factor
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(MUL(_::{distance}, _::FromN::{traffic_factor}))
    ::To(end)
    RETURN result

QUERY test_distance_with_destination_popularity(start: ID, end: ID) =>
    // Weight = distance / destination_popularity (prefer popular destinations)
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(DIV(_::{distance}, _::ToN::{popularity}))
    ::To(end)
    RETURN result

QUERY test_combined_factors(start: ID, end: ID) =>
    // Weight = 0.4*distance + 0.3*source_traffic + 0.3*(1-reliability)
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(
        ADD(
            MUL(_::{distance}, 0.4),
            ADD(
                MUL(_::FromN::{traffic_factor}, 0.3),
                MUL(SUB(1, _::{reliability}), 0.3)
            )
        )
    )
    ::To(end)
    RETURN result

// ============================================================================
// BANDWIDTH-BASED WEIGHTS
// ============================================================================

QUERY test_reciprocal_bandwidth(start: ID, end: ID) =>
    // Weight = 1 / bandwidth (prefer high bandwidth)
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(DIV(1, _::{bandwidth}))
    ::To(end)
    RETURN result

QUERY test_bandwidth_distance_ratio(start: ID, end: ID) =>
    // Weight = distance / bandwidth (optimize for high bandwidth per distance)
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(DIV(_::{distance}, _::{bandwidth}))
    ::To(end)
    RETURN result

// ============================================================================
// TIME-DECAY FUNCTIONS
// ============================================================================

QUERY test_linear_decay(start: ID, end: ID) =>
    // Weight = distance * (1 + days_since_update/100)
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(
        MUL(_::{distance}, ADD(1, DIV(_::{days_since_update}, 100)))
    )
    ::To(end)
    RETURN result

QUERY test_quadratic_decay(start: ID, end: ID) =>
    // Weight = distance * (days_since_update^2 / 1000)
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(
        MUL(_::{distance}, DIV(POW(_::{days_since_update}, 2), 1000))
    )
    ::To(end)
    RETURN result

// ============================================================================
// RELIABILITY-BASED WEIGHTS
// ============================================================================

QUERY test_reliability_weight(start: ID, end: ID) =>
    // Weight = distance / reliability (prefer reliable routes)
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(DIV(_::{distance}, _::{reliability}))
    ::To(end)
    RETURN result

QUERY test_reliability_exponential(start: ID, end: ID) =>
    // Weight = distance * e^(-reliability)
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(
        MUL(_::{distance}, EXP(MUL(SUB(0, 1), _::{reliability})))
    )
    ::To(end)
    RETURN result

// ============================================================================
// GEOMETRIC WEIGHTS
// ============================================================================

QUERY test_sqrt_distance(start: ID, end: ID) =>
    // Weight = sqrt(distance) (non-linear distance penalty)
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(SQRT(_::{distance}))
    ::To(end)
    RETURN result

QUERY test_log_distance(start: ID, end: ID) =>
    // Weight = log(distance + 1) (logarithmic distance)
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(LN(ADD(_::{distance}, 1)))
    ::To(end)
    RETURN result

// ============================================================================
// COMPLEX FORMULAS
// ============================================================================

QUERY test_weighted_score(start: ID, end: ID) =>
    // Complex scoring function
    // score = distance * (0.7 + 0.3 * decay_factor)
    // decay_factor = 0.95^(days/30)
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(
        MUL(
            _::{distance},
            ADD(
                0.7,
                MUL(0.3, POW(0.95, DIV(_::{days_since_update}, 30)))
            )
        )
    )
    ::To(end)
    RETURN result

QUERY test_multi_property_formula(start: ID, end: ID) =>
    // Weight = sqrt(distance^2 + (1000*(1-reliability))^2)
    // Treats distance and unreliability as vector components
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(
        SQRT(
            ADD(
                POW(_::{distance}, 2),
                POW(MUL(1000, SUB(1, _::{reliability})), 2)
            )
        )
    )
    ::To(end)
    RETURN result

// ============================================================================
// ABSOLUTE VALUE TESTS
// ============================================================================

QUERY test_abs_weight(start: ID, end: ID) =>
    // Weight = |distance - 50|
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(ABS(SUB(_::{distance}, 50)))
    ::To(end)
    RETURN result

// ============================================================================
// MODULO-BASED WEIGHTS
// ============================================================================

QUERY test_mod_penalty(start: ID, end: ID) =>
    // Add penalty if days_since_update is odd
    // Weight = distance * (1 + mod(days, 2) * 0.1)
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(
        MUL(_::{distance}, ADD(1, MUL(MOD(_::{days_since_update}, 2), 0.1)))
    )
    ::To(end)
    RETURN result

// ============================================================================
// ROUNDING-BASED WEIGHTS
// ============================================================================

QUERY test_ceil_weight(start: ID, end: ID) =>
    // Weight = ceil(distance / 10) * 10 (round up to nearest 10)
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(MUL(CEIL(DIV(_::{distance}, 10)), 10))
    ::To(end)
    RETURN result

QUERY test_floor_weight(start: ID, end: ID) =>
    // Weight = floor(distance / 10) * 10 (round down to nearest 10)
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(MUL(FLOOR(DIV(_::{distance}, 10)), 10))
    ::To(end)
    RETURN result

// ============================================================================
// CONSTANT WEIGHTS
// ============================================================================

QUERY test_constant_weight(start: ID, end: ID) =>
    // All edges have the same weight (finds path with fewest hops)
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(1)
    ::To(end)
    RETURN result

QUERY test_pi_weight(start: ID, end: ID) =>
    // Weight = distance * π (just to test constants)
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(MUL(_::{distance}, PI()))
    ::To(end)
    RETURN result

// ============================================================================
// NESTED FUNCTION TESTS
// ============================================================================

QUERY test_deeply_nested_weight(start: ID, end: ID) =>
    // Weight = sqrt(abs(distance - round(bandwidth)))
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(
        SQRT(ABS(SUB(_::{distance}, ROUND(_::{bandwidth}))))
    )
    ::To(end)
    RETURN result

// ============================================================================
// TRIGONOMETRIC WEIGHTS (Advanced)
// ============================================================================

QUERY test_sin_based_weight(start: ID, end: ID) =>
    // Weight = distance * (1 + sin(days_since_update / 10))
    // Creates periodic preference pattern
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(
        MUL(_::{distance}, ADD(1, SIN(DIV(_::{days_since_update}, 10))))
    )
    ::To(end)
    RETURN result

// ============================================================================
// SOURCE AND DESTINATION NODE TESTS
// ============================================================================

QUERY test_source_and_dest_properties(start: ID, end: ID) =>
    // Weight = distance * source_traffic * (1 / dest_popularity)
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(
        MUL(
            MUL(_::{distance}, _::FromN::{traffic_factor}),
            DIV(1, _::ToN::{popularity})
        )
    )
    ::To(end)
    RETURN result

QUERY test_property_difference(start: ID, end: ID) =>
    // Weight = distance + |source_traffic - dest_popularity|
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(
        ADD(_::{distance}, ABS(SUB(_::FromN::{traffic_factor}, _::ToN::{popularity})))
    )
    ::To(end)
    RETURN result

// ============================================================================
// STRESS TESTS
// ============================================================================

QUERY test_very_complex_weight(start: ID, end: ID) =>
    // Extremely complex weight calculation
    result <- N<Location>(start)
    ::ShortestPathDijkstras<Route>(
        MUL(
            ADD(
                MUL(_::{distance}, 0.5),
                MUL(POW(0.95, DIV(_::{days_since_update}, 30)), 0.3)
            ),
            ADD(
                DIV(1, _::{bandwidth}),
                MUL(SUB(1, _::{reliability}), 0.2)
            )
        )
    )
    ::To(end)
    RETURN result
