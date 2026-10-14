#
# Copyright (c) 2015-2023 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Attribution Notice under the terms of the Apache License 2.0
#
# This work was created by the collective efforts of the openCypher community.
# Without limiting the terms of Section 6, any Derivative Work that is not
# approved by the public consensus process of the openCypher Implementers Group
# should not be described as “Cypher” (and Cypher® is a registered trademark of
# Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
# proposals for change that have been documented or implemented should only be
# described as "implementation extensions to Cypher" or as "proposed changes to
# Cypher that are not yet approved by the openCypher community".
#

#encoding: utf-8

Feature: Precedence1 - On boolean values

  Scenario: [1] Exclusive disjunction takes precedence over inclusive disjunction
    Given an empty graph
    When executing query:
      """
      RETURN true OR true XOR true AS a,
             true OR (true XOR true) AS b,
             (true OR true) XOR true AS c
      """
    Then the result should be, in any order:
      | a    | b    | c     |
      | true | true | false |
    And no side effects

  Scenario: [2] Conjunction disjunction takes precedence over exclusive disjunction
    Given an empty graph
    When executing query:
      """
      RETURN true XOR false AND false AS a,
             true XOR (false AND false) AS b,
             (true XOR false) AND false AS c
      """
    Then the result should be, in any order:
      | a    | b    | c     |
      | true | true | false |
    And no side effects

  Scenario: [3] Conjunction disjunction takes precedence over inclusive disjunction
    Given an empty graph
    When executing query:
      """
      RETURN true OR false AND false AS a,
             true OR (false AND false) AS b,
             (true OR false) AND false AS c
      """
    Then the result should be, in any order:
      | a    | b    | c     |
      | true | true | false |
    And no side effects

  Scenario: [4] Negation takes precedence over conjunction
    Given an empty graph
    When executing query:
      """
      RETURN NOT true AND false AS a,
             (NOT true) AND false AS b,
             NOT (true AND false) AS c
      """
    Then the result should be, in any order:
      | a     | b     | c    |
      | false | false | true |
    And no side effects

  # Negation and exclusive disjunction are associative over truth values

  Scenario: [5] Negation takes precedence over inclusive disjunction
    Given an empty graph
    When executing query:
      """
      RETURN NOT false OR true AS a,
             (NOT false) OR true AS b,
             NOT (false OR true) AS c
      """
    Then the result should be, in any order:
      | a    | b    | c     |
      | true | true | false |
    And no side effects

  Scenario: [6] Comparison operator takes precedence over boolean negation
    Given an empty graph
    When executing query:
      """
      RETURN NOT false >= false AS a,
             NOT (false >= false) AS b,
             (NOT false) >= false AS c
      """
    Then the result should be, in any order:
      | a     | b     | c    |
      | false | false | true |
    And no side effects

  Scenario: [7] Comparison operator takes precedence over binary boolean operator
    Given an empty graph
    When executing query:
      """
      RETURN true OR false = false AS a,
             true OR (false = false) AS b,
             (true OR false) = false AS c
      """
    Then the result should be, in any order:
      | a    | b    | c     |
      | true | true | false |
    And no side effects

  Scenario: [8] Null predicate takes precedence over comparison operator
    Given an empty graph
    When executing query:
      """
      RETURN false = true IS NULL AS a,
             false = (true IS NULL) AS b,
             (false = true) IS NULL AS c
      """
    Then the result should be, in any order:
      | a    | b    | c     |
      | true | true | false |
    And no side effects

  Scenario: [9] Null predicate takes precedence over negation
    Given an empty graph
    When executing query:
      """
      RETURN NOT false IS NULL AS a,
             NOT (false IS NULL) AS b,
             (NOT false) IS NULL AS c
      """
    Then the result should be, in any order:
      | a    | b    | c     |
      | true | true | false |
    And no side effects

  Scenario: [10] Null predicate takes precedence over boolean operator
    Given an empty graph
    When executing query:
      """
      RETURN true OR false IS NULL AS a,
             true OR (false IS NULL) AS b,
             (true OR false) IS NULL AS c
      """
    Then the result should be, in any order:
      | a    | b    | c     |
      | true | true | false |
    And no side effects

  Scenario: [11] List predicate takes precedence over comparison operator
    Given an empty graph
    When executing query:
      """
      RETURN false = true IN [true, false] AS a,
             false = (true IN [true, false]) AS b,
             (false = true) IN [true, false] AS c
      """
    Then the result should be, in any order:
      | a     | b     | c    |
      | false | false | true |
    And no side effects

  Scenario: [12] List predicate takes precedence over negation
    Given an empty graph
    When executing query:
      """
      RETURN NOT true IN [true, false] AS a,
             NOT (true IN [true, false]) AS b,
             (NOT true) IN [true, false] AS c
      """
    Then the result should be, in any order:
      | a     | b     | c    |
      | false | false | true |
    And no side effects

  Scenario: [13] List predicate takes precedence over boolean operator
    Given an empty graph
    When executing query:
      """
      RETURN false AND true IN [true, false] AS a,
             false AND (true IN [true, false]) AS b,
             (false AND true) IN [true, false] AS c
      """
    Then the result should be, in any order:
      | a     | b     | c    |
      | false | false | true |
    And no side effects

  Scenario: [14] Exclusive disjunction takes precedence over inclusive disjunction in every combination of truth values
    Given an empty graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [true, false, null] AS b
      UNWIND [true, false, null] AS c
      WITH collect((a OR b XOR c) = (a OR (b XOR c))) AS eq,
           collect((a OR b XOR c) <> ((a OR b) XOR c)) AS neq
      RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

  Scenario: [15] Conjunction takes precedence over exclusive disjunction in every combination of truth values
    Given an empty graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [true, false, null] AS b
      UNWIND [true, false, null] AS c
      WITH collect((a XOR b AND c) = (a XOR (b AND c))) AS eq,
           collect((a XOR b AND c) <> ((a XOR b) AND c)) AS neq
      RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

  Scenario: [16] Conjunction takes precedence over inclusive disjunction in every combination of truth values
    Given an empty graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [true, false, null] AS b
      UNWIND [true, false, null] AS c
      WITH collect((a OR b AND c) = (a OR (b AND c))) AS eq,
           collect((a OR b AND c) <> ((a OR b) AND c)) AS neq
      RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

  Scenario: [17] Negation takes precedence over conjunction in every combination of truth values
    Given an empty graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [true, false, null] AS b
      WITH collect((NOT a AND b) = ((NOT a) AND b)) AS eq,
           collect((NOT a AND b) <> (NOT (a AND b))) AS neq
      RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

  # Negation and exclusive disjunction are associative over truth values

  Scenario: [18] Negation takes precedence over inclusive disjunction in every combination of truth values
    Given an empty graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [true, false, null] AS b
      WITH collect((NOT a OR b) = ((NOT a) OR b)) AS eq,
           collect((NOT a OR b) <> (NOT (a OR b))) AS neq
      RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

  Scenario Outline: [19] Comparison operators takes precedence over boolean negation in every combination of truth values
    Given an empty graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [true, false, null] AS b
      WITH collect((NOT a <comp> b) = (NOT (a <comp> b))) AS eq,
           collect((NOT a <comp> b) <> ((NOT a) <comp> b)) AS neq
      RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

    Examples:
      | comp |
      #| =    | # Equality and negation are associative over truth values
      | <=   |
      | >=   |
      | <    |
      | >    |
      #| <>   | # Negated equality and negation are associative over truth values

  Scenario Outline: [20] Pairs of comparison operators and boolean negation that are associative in every combination of truth values
    Given an empty graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [true, false, null] AS b
      WITH collect((NOT (a <comp> b)) = ((NOT a) <comp> b)) AS eq
      RETURN all(x IN eq WHERE x) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

    Examples:
      | comp |
      | =    |
      | <>   |

  Scenario Outline: [21] Comparison operators take precedence over binary boolean operators in every combination of truth values
    Given an empty graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [true, false, null] AS b
      UNWIND [true, false, null] AS c
      WITH collect((a <boolop> b <pred> c) = (a <boolop> (b <pred> c))) AS eq,
           collect((a <boolop> b <pred> c) <> ((a <boolop> b) <pred> c)) AS neq
      RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

    Examples:
      | pred | boolop |
      | =    | OR     |
      #| =    | XOR    | # Equality and exclusive disjunction are associative over truth values
      | =    | AND    |
      | <=   | OR     |
      | <=   | XOR    |
      | <=   | AND    |
      #| >=   | OR     | # Greater or equal and disjunction are associative over truth values
      | >=   | XOR    |
      | >=   | AND    |
      | <    | OR     |
      | <    | XOR    |
      | <    | AND    |
      | >    | OR     |
      | >    | XOR    |
      #| >    | AND    | # Greater and conjunction are associative over truth values
      | <>   | OR     |
      #| <>   | XOR    | # Inequality and exclusive disjunction are associative over truth values; inequality and exclusive disjunction are the same operation over truth values; exclusive disjunction is associative
      | <>   | AND    |

  Scenario Outline: [22] Pairs of comparison operators and binary boolean operators that are associative in every combination of truth values
    Given an empty graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [true, false, null] AS b
      UNWIND [true, false, null] AS c
      WITH collect((a <boolop> (b <pred> c)) = ((a <boolop> b) <pred> c)) AS eq
      RETURN all(x IN eq WHERE x) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

    Examples:
      | pred | boolop |
      | =    | XOR    |
      | >=   | OR     |
      | >    | AND    |
      | <>   | XOR    |

  Scenario Outline: [23] Null predicates take precedence over comparison operators in every combination of truth values
    Given an empty graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [true, false, null] AS b
      WITH collect((a <comp> b <nullpred>) = (a <comp> (b <nullpred>))) AS eq,
           collect((a <comp> b <nullpred>) <> ((a <comp> b) <nullpred>)) AS neq
      RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

    Examples:
      | comp | nullpred    |
      | =    | IS NULL     |
      | =    | IS NOT NULL |
      | <=   | IS NULL     |
      | <=   | IS NOT NULL |
      | >=   | IS NULL     |
      | >=   | IS NOT NULL |
      | <    | IS NULL     |
      | <    | IS NOT NULL |
      | >    | IS NULL     |
      | >    | IS NOT NULL |
      | <>   | IS NULL     |
      | <>   | IS NOT NULL |

  Scenario Outline: [24] Null predicates take precedence over boolean negation on every truth values
    Given an empty graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [true, false, null] AS b
      WITH collect((NOT a <nullpred>) = (NOT (a <nullpred>))) AS eq,
           collect((NOT a <nullpred>) <> ((NOT a) <nullpred>)) AS neq
      RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

    Examples:
      | nullpred    |
      | IS NULL     |
      | IS NOT NULL |

  Scenario Outline: [25] Null predicates take precedence over binary boolean operators in every combination of truth values
    Given an empty graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [true, false, null] AS b
      WITH collect((a <boolop> b <nullpred>) = (a <boolop> (b <nullpred>))) AS eq,
           collect((a <boolop> b <nullpred>) <> ((a <boolop> b) <nullpred>)) AS neq
      RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

    Examples:
      | boolop | nullpred    |
      | OR     | IS NULL     |
      | OR     | IS NOT NULL |
      | XOR    | IS NULL     |
      | XOR    | IS NOT NULL |
      | AND    | IS NULL     |
      | AND    | IS NOT NULL |

  Scenario Outline: [26] List predicate takes precedence over comparison operators in every combination of truth values
    Given an empty graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [true, false, null] AS b
      UNWIND [[], [true], [false], [null], [true, false], [true, false, null]] AS c
      WITH collect((a <comp> b IN c) = (a <comp> (b IN c))) AS eq,
           collect((a <comp> b IN c) <> ((a <comp> b) IN c)) AS neq
      RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

    Examples:
      | comp |
      | =    |
      | <=   |
      | >=   |
      | <    |
      | >    |
      | <>   |

  Scenario: [27] List predicate takes precedence over negation in every combination of truth values
    Given an empty graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [[], [true], [false], [null], [true, false], [true, false, null]] AS b
      WITH collect((NOT a IN b) = (NOT (a IN b))) AS eq,
           collect((NOT a IN b) <> ((NOT a) IN b)) AS neq
      RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

  Scenario Outline: [28] List predicate takes precedence over binary boolean operators in every combination of truth values
    Given an empty graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [true, false, null] AS b
      UNWIND [[], [true], [false], [null], [true, false], [true, false, null]] AS c
      WITH collect((a <boolop> b IN c) = (a <boolop> (b IN c))) AS eq,
           collect((a <boolop> b IN c) <> ((a <boolop> b) IN c)) AS neq
      RETURN all(x IN eq WHERE x) AND any(x IN neq WHERE x) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

    Examples:
      | boolop |
      | OR     |
      | XOR    |
      | AND    |
