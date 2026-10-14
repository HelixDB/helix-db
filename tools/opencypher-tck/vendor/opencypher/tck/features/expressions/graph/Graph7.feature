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

Feature: Graph7 - Dynamic property access
  # Accessing a property of a node or edge by using a dynamically-computed string value as the key; e.g. allowing for the key to be passed in as a parameter

  Scenario: [1] Execute n['name'] in read queries
    Given any graph
    And having executed:
      """
      CREATE ({name: 'Apa'})
      """
    When executing query:
      """
      MATCH (n {name: 'Apa'})
      RETURN n['nam' + 'e'] AS value
      """
    Then the result should be, in any order:
      | value |
      | 'Apa' |
    And no side effects

  Scenario: [2] Execute n['name'] in update queries
    Given any graph
    When executing query:
      """
      CREATE (n {name: 'Apa'})
      RETURN n['nam' + 'e'] AS value
      """
    Then the result should be, in any order:
      | value |
      | 'Apa' |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 1 |

  Scenario: [3] Use dynamic property lookup based on parameters when there is lhs type information
    Given any graph
    And parameters are:
      | idx | 'name' |
    When executing query:
      """
      CREATE (n {name: 'Apa'})
      RETURN n[$idx] AS value
      """
    Then the result should be, in any order:
      | value |
      | 'Apa' |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 1 |
