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

Feature: Graph9 - Retrieve all properties as a property map

  Scenario: [1] `properties()` on a node
    Given an empty graph
    And having executed:
      """
      CREATE (n:Person {name: 'Popeye', level: 9001})
      """
    When executing query:
      """
      MATCH (p:Person)
      RETURN properties(p) AS m
      """
    Then the result should be, in any order:
      | m                             |
      | {name: 'Popeye', level: 9001} |
    And no side effects

  Scenario: [2] `properties()` on a relationship
    Given an empty graph
    And having executed:
      """
      CREATE (n)-[:R {name: 'Popeye', level: 9001}]->(n)
      """
    When executing query:
      """
      MATCH ()-[r:R]->()
      RETURN properties(r) AS m
      """
    Then the result should be, in any order:
      | m                             |
      | {name: 'Popeye', level: 9001} |
    And no side effects

  Scenario: [3] `properties()` on null
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH (n:DoesNotExist)
      OPTIONAL MATCH (n)-[r:NOT_THERE]->()
      RETURN properties(n), properties(r), properties(null)
      """
    Then the result should be, in any order:
      | properties(n) | properties(r) | properties(null) |
      | null          | null          | null             |
    And no side effects

  Scenario: [4] `properties()` on a map
    Given any graph
    When executing query:
      """
      RETURN properties({name: 'Popeye', level: 9001}) AS m
      """
    Then the result should be, in any order:
      | m                             |
      | {name: 'Popeye', level: 9001} |
    And no side effects

  Scenario: [5] `properties()` failing on an integer literal
    Given any graph
    When executing query:
      """
      RETURN properties(1)
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType

  Scenario: [6] `properties()` failing on a string literal
    Given any graph
    When executing query:
      """
      RETURN properties('Cypher')
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType

  Scenario: [7] `properties()` failing on a list of booleans
    Given any graph
    When executing query:
      """
      RETURN properties([true, false])
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType
