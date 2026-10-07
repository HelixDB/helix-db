//! Arena parsing of the planner's own scalability fixtures yields the same
//! requests as owned parsing, on both JSON backends.

use helix_ast::arena::{Bump, IntoOwned};
use helix_ast::query::{ArenaQueryRequest, QueryRequest};

#[test]
fn planner_fixtures_parse_identically_into_the_arena() {
    let fixtures = helix_planner::experiments::default_planning_scalability_fixtures();
    assert!(!fixtures.is_empty());
    for fixture in fixtures {
        let case = fixture.case();
        let request = match (case.read_batch(), case.write_batch()) {
            (Some(batch), _) => QueryRequest::read(batch.clone()),
            (None, Some(batch)) => QueryRequest::write(batch.clone()),
            (None, None) => panic!("{fixture:?} has no batch"),
        };
        let json = request.to_json_bytes().expect("fixtures serialize");
        let owned = QueryRequest::from_json_slice(&json).expect("fixtures parse");
        assert_eq!(owned, request, "{fixture:?} round-trips");
        let bump = Bump::new();
        let arena = ArenaQueryRequest::from_json_slice(&bump, &json).expect("fixtures parse");
        assert_eq!(arena.into_owned(), owned, "{fixture:?} with sonic-rs");
        let simd = ArenaQueryRequest::from_json_slice_mut(&bump, &mut json.clone())
            .expect("fixtures parse");
        assert_eq!(simd.into_owned(), owned, "{fixture:?} with simd-json");
    }
}
