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

Feature: WithWhere4 - Non-Equi-Joins on variables

  Scenario: [1] Join nodes on inequality
    Given an empty graph
    And having executed:
      """
      CREATE (:A), (:B)
      """
    When executing query:
      """
      MATCH (a), (b)
      WITH a, b
      WHERE a <> b
      RETURN a, b
      """
    Then the result should be, in any order:
      | a    | b    |
      | (:A) | (:B) |
      | (:B) | (:A) |
    And no side effects

  Scenario: [2] Join with disjunctive multi-part predicates including patterns
    Given an empty graph
    And having executed:
      """
      CREATE (a:TheLabel {id: 0}), (b:TheLabel {id: 1}), (c:TheLabel {id: 2})
      CREATE (a)-[:T]->(b),
             (b)-[:T]->(c)
      """
    When executing query:
      """
      MATCH (a), (b)
      WITH a, b
      WHERE a.id = 0
        AND (a)-[:T]->(b:TheLabel)
        OR (a)-[:T*]->(b:MissingLabel)
      RETURN DISTINCT b
      """
    Then the result should be, in any order:
      | b                   |
      | (:TheLabel {id: 1}) |
    And no side effects
