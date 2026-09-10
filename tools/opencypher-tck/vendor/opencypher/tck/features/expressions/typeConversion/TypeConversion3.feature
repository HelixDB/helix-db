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

Feature: TypeConversion3 - To Float

  Scenario: [1] `toFloat()` on mixed number types
    Given any graph
    When executing query:
      """
      WITH [3.4, 3] AS numbers
      RETURN [n IN numbers | toFloat(n)] AS float_numbers
      """
    Then the result should be, in any order:
      | float_numbers |
      | [3.4, 3.0]    |
    And no side effects

  Scenario: [2] `toFloat()` returning null on non-numerical string
    Given any graph
    When executing query:
      """
      WITH 'foo' AS foo_string, '' AS empty_string
      RETURN toFloat(foo_string) AS foo, toFloat(empty_string) AS empty
      """
    Then the result should be, in any order:
      | foo  | empty |
      | null | null  |
    And no side effects

  Scenario: [3] `toFloat()` handling Any type
    Given any graph
    When executing query:
      """
      WITH [3.4, 3, '5'] AS numbers
      RETURN [n IN numbers | toFloat(n)] AS float_numbers
      """
    Then the result should be, in any order:
      | float_numbers   |
      | [3.4, 3.0, 5.0] |
    And no side effects

  Scenario: [4] `toFloat()` on a list of strings
    Given any graph
    When executing query:
      """
      WITH ['1', '2', 'foo'] AS numbers
      RETURN [n IN numbers | toFloat(n)] AS float_numbers
      """
    Then the result should be, in any order:
      | float_numbers    |
      | [1.0, 2.0, null] |
    And no side effects

  Scenario: [5] `toFloat()` on node property
    Given an empty graph
    And having executed:
      """
      CREATE (:Movie {rating: 4})
      """
    When executing query:
      """
      MATCH (m:Movie { rating: 4 })
      WITH *
      MATCH (n)
      RETURN toFloat(n.rating) AS float
      """
    Then the result should be, in any order:
      | float |
      | 4.0   |
    And no side effects

  Scenario Outline: [6] Fail `toFloat()` on invalid types #Example: <exampleName>
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:T]->()
      """
    When executing query:
      """
      MATCH p = (n)-[r:T]->()
      RETURN [x IN [1.0, <invalid>] | toFloat(x) ] AS list
      """
    Then a TypeError should be raised at runtime: InvalidArgumentValue

    Examples:
      | invalid | exampleName  |
      | true    | boolean      |
      | []      | list         |
      | {}      | map          |
      | n       | node         |
      | r       | relationship |
      | p       | path         |
