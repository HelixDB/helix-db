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

Feature: ReturnSkipLimit3 - Skip and limit

  Scenario: [1] Get rows in the middle
    Given an empty graph
    And having executed:
      """
      CREATE ({name: 'A'}),
        ({name: 'B'}),
        ({name: 'C'}),
        ({name: 'D'}),
        ({name: 'E'})
      """
    When executing query:
      """
      MATCH (n)
      RETURN n
      ORDER BY n.name ASC
      SKIP 2
      LIMIT 2
      """
    Then the result should be, in order:
      | n             |
      | ({name: 'C'}) |
      | ({name: 'D'}) |
    And no side effects

  Scenario: [2] Get rows in the middle by param
    Given an empty graph
    And having executed:
      """
      CREATE ({name: 'A'}),
        ({name: 'B'}),
        ({name: 'C'}),
        ({name: 'D'}),
        ({name: 'E'})
      """
    And parameters are:
      | s | 2 |
      | l | 2 |
    When executing query:
      """
      MATCH (n)
      RETURN n
      ORDER BY n.name ASC
      SKIP $s
      LIMIT $l
      """
    Then the result should be, in order:
      | n             |
      | ({name: 'C'}) |
      | ({name: 'D'}) |
    And no side effects

  Scenario: [3] Limiting amount of rows when there are fewer left than the LIMIT argument
    Given an empty graph
    And having executed:
      """
      UNWIND range(0, 15) AS i
      CREATE ({count: i})
      """
    When executing query:
      """
      MATCH (a)
      RETURN a.count
        ORDER BY a.count
        SKIP 10
        LIMIT 10
      """
    Then the result should be, in order:
      | a.count |
      | 10      |
      | 11      |
      | 12      |
      | 13      |
      | 14      |
      | 15      |
    And no side effects
