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

Feature: Graph6 - Static property access
  # Accessing a property of a node or edge by using a symbolic name as the key.

  Scenario: [1] Statically access a property of a non-null node
    Given an empty graph
    And having executed:
      """
      CREATE ({existing: 42, missing: null})
      """
    When executing query:
      """
      MATCH (n)
      RETURN n.missing, n.missingToo, n.existing
      """
    Then the result should be, in any order:
      | n.missing | n.missingToo | n.existing |
      | null      | null         | 42         |
    And no side effects

  Scenario: [2] Statically access a property of a optional non-null node
    Given an empty graph
    And having executed:
      """
      CREATE ({existing: 42, missing: null})
      """
    When executing query:
      """
      OPTIONAL MATCH (n)
      RETURN n.missing, n.missingToo, n.existing
      """
    Then the result should be, in any order:
      | n.missing | n.missingToo | n.existing |
      | null      | null         | 42         |
    And no side effects

  Scenario: [3] Statically access a property of a null node
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH (n)
      RETURN n.missing
      """
    Then the result should be, in any order:
      | n.missing |
      | null      |
    And no side effects

  Scenario: [4] Statically access a property of a node resulting from an expression
    Given an empty graph
    And having executed:
      """
      CREATE ({existing: 42, missing: null})
      """
    When executing query:
      """
      MATCH (n)
      WITH [123, n] AS list
      RETURN (list[1]).missing, (list[1]).missingToo, (list[1]).existing
      """
    Then the result should be, in any order:
      | (list[1]).missing | (list[1]).missingToo | (list[1]).existing |
      | null              | null                 | 42                 |
    And no side effects

  Scenario: [5] Statically access a property of a non-null relationship
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:REL {existing: 42, missing: null}]->()
      """
    When executing query:
      """
      MATCH ()-[r]->()
      RETURN r.missing, r.missingToo, r.existing
      """
    Then the result should be, in any order:
      | r.missing | r.missingToo | r.existing |
      | null      | null         | 42         |
    And no side effects

  Scenario: [6] Statically access a property of a optional non-null relationship
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:REL {existing: 42, missing: null}]->()
      """
    When executing query:
      """
      OPTIONAL MATCH ()-[r]->()
      RETURN r.missing, r.missingToo, r.existing
      """
    Then the result should be, in any order:
      | r.missing | r.missingToo | r.existing |
      | null      | null         | 42         |
    And no side effects

  Scenario: [7] Statically access a property of a null relationship
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH ()-[r]->()
      RETURN r.missing
      """
    Then the result should be, in any order:
      | r.missing |
      | null      |
    And no side effects

  Scenario: [8] Statically access a property of a relationship resulting from an expression
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:REL {existing: 42, missing: null}]->()
      """
    When executing query:
      """
      MATCH ()-[r]->()
      WITH [123, r] AS list
      RETURN (list[1]).missing, (list[1]).missingToo, (list[1]).existing
      """
    Then the result should be, in any order:
      | (list[1]).missing | (list[1]).missingToo | (list[1]).existing |
      | null              | null                 | 42                 |
    And no side effects

  Scenario Outline: [9] Fail when performing property access on a non-graph element
    Given any graph
    When executing query:
      """
      WITH <exp> AS nonGraphElement
      RETURN nonGraphElement.num
      """
    Then a TypeError should be raised at compile time: InvalidArgumentType

    Examples:
      | exp         |
      | 123         |
      | 42.45       |
      | true        |
      | false       |
      | 'string'    |
      | [123, true] |
