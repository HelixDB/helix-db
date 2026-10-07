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

Feature: TypeConversion2 - To Integer

  Scenario: [1] `toInteger()` on float
    Given any graph
    When executing query:
      """
      WITH 82.9 AS weight
      RETURN toInteger(weight)
      """
    Then the result should be, in any order:
      | toInteger(weight) |
      | 82                |
    And no side effects

  Scenario: [2] `toInteger()` returning null on non-numerical string
    Given any graph
    When executing query:
      """
      WITH 'foo' AS foo_string, '' AS empty_string
      RETURN toInteger(foo_string) AS foo, toInteger(empty_string) AS empty
      """
    Then the result should be, in any order:
      | foo  | empty |
      | null | null  |
    And no side effects

  Scenario: [3] `toInteger()` handling mixed number types
    Given any graph
    When executing query:
      """
      WITH [2, 2.9] AS numbers
      RETURN [n IN numbers | toInteger(n)] AS int_numbers
      """
    Then the result should be, in any order:
      | int_numbers |
      | [2, 2]      |
    And no side effects

  Scenario: [4] `toInteger()` handling Any type
    Given any graph
    When executing query:
      """
      WITH [2, 2.9, '1.7'] AS things
      RETURN [n IN things | toInteger(n)] AS int_numbers
      """
    Then the result should be, in any order:
      | int_numbers |
      | [2, 2, 1]   |
    And no side effects

  Scenario: [5] `toInteger()` on a list of strings
    Given any graph
    When executing query:
      """
      WITH ['2', '2.9', 'foo'] AS numbers
      RETURN [n IN numbers | toInteger(n)] AS int_numbers
      """
    Then the result should be, in any order:
      | int_numbers  |
      | [2, 2, null] |
    And no side effects

  Scenario: [6] `toInteger()` on a complex-typed expression
    Given any graph
    And parameters are:
      | param | 1 |
    When executing query:
      """
      RETURN toInteger(1 - $param) AS result
      """
    Then the result should be, in any order:
      | result |
      | 0      |
    And no side effects

  Scenario: [7] `toInteger()` on node property
    Given an empty graph
    And having executed:
      """
      CREATE (:Person {name: '42'})
      """
    When executing query:
      """
      MATCH (p:Person { name: '42' })
      WITH *
      MATCH (n)
      RETURN toInteger(n.name) AS name
      """
    Then the result should be, in any order:
      | name |
      | 42   |
    And no side effects

  Scenario Outline: [8] Fail `toInteger()` on invalid types #Example: <exampleName>
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:T]->()
      """
    When executing query:
      """
      MATCH p = (n)-[r:T]->()
      RETURN [x IN [1, <invalid>] | toInteger(x) ] AS list
      """
    Then a TypeError should be raised at runtime: InvalidArgumentValue

    Examples:
      | invalid | exampleName  |
      | []      | list         |
      | {}      | map          |
      | n       | node         |
      | r       | relationship |
      | p       | path         |
