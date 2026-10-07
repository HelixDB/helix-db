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

Feature: Set2 - Set a Property to Null

  Scenario: [1] Setting a node property to null removes the existing property
    Given an empty graph
    And having executed:
      """
      CREATE (:A {property1: 23, property2: 46})
      """
    When executing query:
      """
      MATCH (n:A)
      SET n.property1 = null
      RETURN n
      """
    Then the result should be, in any order:
      | n                    |
      | (:A {property2: 46}) |
    And the side effects should be:
      | -properties | 1 |

  Scenario: [2] Setting a node property to null removes the existing property, but not before SET
    Given an empty graph
    And having executed:
      """
      CREATE (:A {name: 'Michael', age: 35})
      """
    When executing query:
      """
      MATCH (n)
      WHERE n.name = 'Michael'
      SET n.name = null
      RETURN n
      """
    Then the result should be, in any order:
      | n              |
      | (:A {age: 35}) |
    And the side effects should be:
      | -properties | 1 |

  Scenario: [3] Setting a relationship property to null removes the existing property
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:REL {property1: 12, property2: 24}]->()
      """
    When executing query:
      """
      MATCH ()-[r]->()
      SET r.property1 = null
      RETURN r
      """
    Then the result should be, in any order:
      | r                      |
      | [:REL {property2: 24}] |
    And the side effects should be:
      | -properties | 1 |
