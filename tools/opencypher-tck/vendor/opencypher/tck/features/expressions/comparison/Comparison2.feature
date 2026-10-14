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

Feature: Comparison2 - Half-bounded Range

  Scenario: [1] Comparing strings and integers using > in an AND'd predicate
    Given an empty graph
    And having executed:
      """
      CREATE (root:Root)-[:T]->(:Child {var: 0}),
             (root)-[:T]->(:Child {var: 'xx'}),
             (root)-[:T]->(:Child)
      """
    When executing query:
      """
      MATCH (:Root)-->(i:Child)
      WHERE i.var IS NOT NULL AND i.var > 'x'
      RETURN i.var
      """
    Then the result should be, in any order:
      | i.var |
      | 'xx'  |
    And no side effects

  Scenario: [2] Comparing strings and integers using > in a OR'd predicate
    Given an empty graph
    And having executed:
      """
      CREATE (root:Root)-[:T]->(:Child {var: 0}),
             (root)-[:T]->(:Child {var: 'xx'}),
             (root)-[:T]->(:Child)
      """
    When executing query:
      """
      MATCH (:Root)-->(i:Child)
      WHERE i.var IS NULL OR i.var > 'x'
      RETURN i.var
      """
    Then the result should be, in any order:
      | i.var |
      | 'xx'  |
      | null  |
    And no side effects

  Scenario Outline: [3] Comparing across types yields null, except numbers
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:T]->()
      """
    When executing query:
      """
      MATCH p = (n)-[r]->()
      WITH [n, r, p, '', 1, 3.14, true, null, [], {}] AS types
      UNWIND range(0, size(types) - 1) AS i
      UNWIND range(0, size(types) - 1) AS j
      WITH types[i] AS lhs, types[j] AS rhs
      WHERE i <> j
      WITH lhs, rhs, lhs <operator> rhs AS result
      WHERE result
      RETURN lhs, rhs
      """
    Then the result should be, in any order:
      | lhs   | rhs   |
      | <lhs> | <rhs> |
    And no side effects

    Examples:
      | operator | lhs  | rhs  |
      | <        | 1    | 3.14 |
      | <=       | 1    | 3.14 |
      | >=       | 3.14 | 1    |
      | >        | 3.14 | 1    |

  Scenario Outline: [4] Comparing lists
    Given an empty graph
    When executing query:
      """
      RETURN <lhs> >= <rhs> AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    And no side effects

    Examples:
      | lhs       | rhs       | result |
      | [1, 0]    | [1]       | true   |
      | [1, null] | [1]       | true   |
      | [1, 2]    | [1, null] | null   |
      | [1, 'a']  | [1, null] | null   |
      | [1, 2]    | [3, null] | false  |

  Scenario Outline: [5] Comparing NaN
    Given an empty graph
    When executing query:
      """
      RETURN <lhs> > <rhs> AS gt, <lhs> >= <rhs> AS gtE, <lhs> < <rhs> AS lt, <lhs> <= <rhs> AS ltE
      """
    Then the result should be, in any order:
      | gt       | gtE      | lt       | ltE      |
      | <result> | <result> | <result> | <result> |
    And no side effects

    Examples:
      | lhs       | rhs       | result |
      | 0.0 / 0.0 | 1         | false  |
      | 0.0 / 0.0 | 1.0       | false  |
      | 0.0 / 0.0 | 0.0 / 0.0 | false  |
      | 0.0 / 0.0 | 'a'       | null   |

  Scenario Outline: [6] Comparability between numbers and strings
    Given any graph
    When executing query:
      """
      RETURN <lhs> < <rhs> AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    And no side effects

    Examples:
      | lhs   | rhs | result |
      | 1.0   | 1.0 | false  |
      | 1     | 1.0 | false  |
      | '1.0' | 1.0 | null   |
      | '1'   | 1   | null   |
