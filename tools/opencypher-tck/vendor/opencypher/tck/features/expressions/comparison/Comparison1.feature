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

Feature: Comparison1 - Equality

  Scenario: [1] Number-typed integer comparison
    Given an empty graph
    And having executed:
      """
      CREATE ({id: 0})
      """
    When executing query:
      """
      WITH collect([0, 0.0]) AS numbers
      UNWIND numbers AS arr
      WITH arr[0] AS expected
      MATCH (n) WHERE toInteger(n.id) = expected
      RETURN n
      """
    Then the result should be, in any order:
      | n         |
      | ({id: 0}) |
    And no side effects

  Scenario: [2] Number-typed float comparison
    Given an empty graph
    And having executed:
      """
      CREATE ({id: 0})
      """
    When executing query:
      """
      WITH collect([0.5, 0]) AS numbers
      UNWIND numbers AS arr
      WITH arr[0] AS expected
      MATCH (n) WHERE toInteger(n.id) = expected
      RETURN n
      """
    Then the result should be, in any order:
      | n |
    And no side effects

  Scenario: [3] Any-typed string comparison
    Given an empty graph
    And having executed:
      """
      CREATE ({id: 0})
      """
    When executing query:
      """
      WITH collect(['0', 0]) AS things
      UNWIND things AS arr
      WITH arr[0] AS expected
      MATCH (n) WHERE toInteger(n.id) = expected
      RETURN n
      """
    Then the result should be, in any order:
      | n |
    And no side effects

  Scenario: [4] Comparing nodes to nodes
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      MATCH (a)
      WITH a
      MATCH (b)
      WHERE a = b
      RETURN count(b)
      """
    Then the result should be, in any order:
      | count(b) |
      | 1        |
    And no side effects

  Scenario: [5] Comparing relationships to relationships
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:T]->()
      """
    When executing query:
      """
      MATCH ()-[a]->()
      WITH a
      MATCH ()-[b]->()
      WHERE a = b
      RETURN count(b)
      """
    Then the result should be, in any order:
      | count(b) |
      | 1        |
    And no side effects

  Scenario Outline: [6] Comparing lists to lists
    Given an empty graph
    When executing query:
      """
      RETURN <lhs> = <rhs> AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    And no side effects

    Examples:
      | lhs           | rhs           | result |
      | [1, 2]        | [1]           | false  |
      | [null]        | [1]           | null   |
      | ['a']         | [1]           | false  |
      | [[1]]         | [[1], [null]] | false  |
      | [[1], [2]]    | [[1], [null]] | null   |
      | [[1], [2, 3]] | [[1], [null]] | false  |

  Scenario Outline: [7] Comparing maps to maps
    Given an empty graph
    When executing query:
      """
      RETURN <lhs> = <rhs> AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    And no side effects

    Examples:
      | lhs             | rhs                | result |
      | {}              | {}                 | true   |
      | {k: true}       | {k: true}          | true   |
      | {k: 1}          | {k: 1}             | true   |
      | {k: 1.0}        | {k: 1.0}           | true   |
      | {k: 'abc'}      | {k: 'abc'}         | true   |
      | {k: 'a', l: 2}  | {k: 'a', l: 2}     | true   |
      | {}              | {k: null}          | false  |
      | {k: null}       | {}                 | false  |
      | {k: 1}          | {k: 1, l: null}    | false  |
      | {k: null, l: 1} | {l: 1}             | false  |
      | {k: null}       | {k: null, l: null} | false  |
      | {k: null}       | {k: null}          | null   |
      | {k: 1}          | {k: null}          | null   |
      | {k: 1, l: null} | {k: null, l: null} | null   |
      | {k: 1, l: null} | {k: null, l: 1}    | null   |
      | {k: 1, l: null} | {k: 1, l: 1}       | null   |

  Scenario Outline: [8] Equality and inequality of NaN
    Given any graph
    When executing query:
      """
      RETURN <lhs> = <rhs> AS isEqual, <lhs> <> <rhs> AS isNotEqual
      """
    Then the result should be, in any order:
      | isEqual | isNotEqual |
      | false   | true       |
    And no side effects

    Examples:
      | lhs       | rhs       |
      | 0.0 / 0.0 | 1         |
      | 0.0 / 0.0 | 1.0       |
      | 0.0 / 0.0 | 0.0 / 0.0 |
      | 0.0 / 0.0 | 'a'       |

  Scenario Outline: [9] Equality between strings and numbers
    Given any graph
    When executing query:
      """
      RETURN <lhs> = <rhs> AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    And no side effects

    Examples:
      | lhs   | rhs | result  |
      | 1.0   | 1.0 | true    |
      | 1     | 1.0 | true    |
      | '1.0' | 1.0 | false   |
      | '1'   | 1   | false   |

  Scenario: [10] Handling inlined equality of large integer
    Given an empty graph
    And having executed:
      """
      CREATE (:TheLabel {id: 4611686018427387905})
      """
    When executing query:
      """
      MATCH (p:TheLabel {id: 4611686018427387905})
      RETURN p.id
      """
    Then the result should be, in any order:
      | p.id                |
      | 4611686018427387905 |
    And no side effects

  Scenario: [11] Handling explicit equality of large integer
    Given an empty graph
    And having executed:
      """
      CREATE (:TheLabel {id: 4611686018427387905})
      """
    When executing query:
      """
      MATCH (p:TheLabel)
      WHERE p.id = 4611686018427387905
      RETURN p.id
      """
    Then the result should be, in any order:
      | p.id                |
      | 4611686018427387905 |
    And no side effects

  Scenario: [12] Handling inlined equality of large integer, non-equal values
    Given an empty graph
    And having executed:
      """
      CREATE (:TheLabel {id: 4611686018427387905})
      """
    When executing query:
      """
      MATCH (p:TheLabel {id : 4611686018427387900})
      RETURN p.id
      """
    Then the result should be, in any order:
      | p.id |
    And no side effects

  Scenario: [13] Handling explicit equality of large integer, non-equal values
    Given an empty graph
    And having executed:
      """
      CREATE (:TheLabel {id: 4611686018427387905})
      """
    When executing query:
      """
      MATCH (p:TheLabel)
      WHERE p.id = 4611686018427387900
      RETURN p.id
      """
    Then the result should be, in any order:
      | p.id |
    And no side effects

  Scenario: [14] Direction of traversed relationship is not significant for path equality, simple
    Given an empty graph
    And having executed:
      """
      CREATE (n:A)-[:LOOP]->(n)
      """
    When executing query:
      """
      MATCH p1 = (:A)-->()
      MATCH p2 = (:A)<--()
      RETURN p1 = p2
      """
    Then the result should be, in any order:
      | p1 = p2 |
      | true    |
    And no side effects

  Scenario: [15] It is unknown - i.e. null - if a null is equal to a null
    Given any graph
    When executing query:
      """
      RETURN null = null AS value
      """
    Then the result should be, in any order:
      | value |
      | null  |
    And no side effects

  Scenario: [16] It is unknown - i.e. null - if a null is not equal to a null
    Given any graph
    When executing query:
      """
      RETURN null <> null AS value
      """
    Then the result should be, in any order:
      | value |
      | null  |
    And no side effects

  Scenario: [17] Failing when comparing to an undefined variable
    Given any graph
    When executing query:
      """
      MATCH (s)
      WHERE s.name = undefinedVariable
        AND s.age = 10
      RETURN s
      """
    Then a SyntaxError should be raised at compile time: UndefinedVariable
