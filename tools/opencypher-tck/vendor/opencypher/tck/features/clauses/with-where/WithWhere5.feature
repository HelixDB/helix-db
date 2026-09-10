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

Feature: WithWhere5 - Filter on predicate resulting in null

  Scenario: [1] Filter out on null
    Given an empty graph
    And having executed:
      """
      CREATE (root:Root {name: 'x'}),
             (child1:TextNode {var: 'text'}),
             (child2:IntNode {var: 0})
      CREATE (root)-[:T]->(child1),
             (root)-[:T]->(child2)
      """
    When executing query:
      """
      MATCH (:Root {name: 'x'})-->(i:TextNode)
      WITH i
      WHERE i.var > 'te'
      RETURN i
      """
    Then the result should be, in any order:
      | i                         |
      | (:TextNode {var: 'text'}) |
    And no side effects

  Scenario: [2] Filter out on null if the AND'd predicate evaluates to false
    Given an empty graph
    And having executed:
      """
      CREATE (root:Root {name: 'x'}),
             (child1:TextNode {var: 'text'}),
             (child2:IntNode {var: 0})
      CREATE (root)-[:T]->(child1),
             (root)-[:T]->(child2)
      """
    When executing query:
      """
      MATCH (:Root {name: 'x'})-->(i:TextNode)
      WITH i
      WHERE i.var > 'te' AND i:TextNode
      RETURN i
      """
    Then the result should be, in any order:
      | i                         |
      | (:TextNode {var: 'text'}) |
    And no side effects

  Scenario: [3] Filter out on null if the AND'd predicate evaluates to true
    Given an empty graph
    And having executed:
      """
      CREATE (root:Root {name: 'x'}),
             (child1:TextNode {var: 'text'}),
             (child2:IntNode {var: 0})
      CREATE (root)-[:T]->(child1),
             (root)-[:T]->(child2)
      """
    When executing query:
      """
      MATCH (:Root {name: 'x'})-->(i:TextNode)
      WITH i
      WHERE i.var > 'te' AND i.var IS NOT NULL
      RETURN i
      """
    Then the result should be, in any order:
      | i                         |
      | (:TextNode {var: 'text'}) |
    And no side effects

  Scenario: [4] Do not filter out on null if the OR'd predicate evaluates to true
    Given an empty graph
    And having executed:
      """
      CREATE (root:Root {name: 'x'}),
             (child1:TextNode {var: 'text'}),
             (child2:IntNode {var: 0})
      CREATE (root)-[:T]->(child1),
             (root)-[:T]->(child2)
      """
    When executing query:
      """
      MATCH (:Root {name: 'x'})-->(i)
      WITH i
      WHERE i.var > 'te' OR i.var IS NOT NULL
      RETURN i
      """
    Then the result should be, in any order:
      | i                         |
      | (:TextNode {var: 'text'}) |
      | (:IntNode {var: 0})       |
    And no side effects

