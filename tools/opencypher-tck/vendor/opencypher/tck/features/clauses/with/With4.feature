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

Feature: With4 - Variable aliasing
  # correctly aliasing variables

  Scenario: [1] Aliasing relationship variable
    Given an empty graph
    And having executed:
      """
      CREATE ()-[:T1]->(),
             ()-[:T2]->()
      """
    When executing query:
      """
      MATCH ()-[r1]->()
      WITH r1 AS r2
      RETURN r2 AS rel
      """
    Then the result should be, in any order:
      | rel   |
      | [:T1] |
      | [:T2] |
    And no side effects

  Scenario: [2] Aliasing expression to new variable name
    Given an empty graph
    And having executed:
      """
      CREATE (:Begin {num: 42}),
             (:End {num: 42}),
             (:End {num: 3})
      """
    When executing query:
      """
      MATCH (a:Begin)
      WITH a.num AS property
      MATCH (b:End)
      WHERE property = b.num
      RETURN b
      """
    Then the result should be, in any order:
      | b                |
      | (:End {num: 42}) |
    And no side effects

  Scenario: [3] Aliasing expression to existing variable name
    Given an empty graph
    And having executed:
      """
      CREATE ({num: 1, name: 'King Kong'}),
        ({num: 2, name: 'Ann Darrow'})
      """
    When executing query:
      """
      MATCH (n)
      WITH n.name AS n
      RETURN n
      """
    Then the result should be, in any order:
      | n            |
      | 'Ann Darrow' |
      | 'King Kong'  |
    And no side effects

  Scenario: [4] Fail when forwarding multiple aliases with the same name
    Given any graph
    When executing query:
      """
      WITH 1 AS a, 2 AS a
      RETURN a
      """
    Then a SyntaxError should be raised at compile time: ColumnNameConflict

  Scenario: [5] Fail when not aliasing expressions in WITH
    Given any graph
    When executing query:
      """
      MATCH (a)
      WITH a, count(*)
      RETURN a
      """
    Then a SyntaxError should be raised at compile time: NoExpressionAlias

  Scenario: [6] Reusing variable names in WITH
    Given an empty graph
    And having executed:
      """
      CREATE (a:Person), (b:Person), (m:Message {id: 10})
      CREATE (a)-[:LIKE {creationDate: 20160614}]->(m)-[:POSTED_BY]->(b)
      """
    When executing query:
      """
      MATCH (person:Person)<--(message)<-[like]-(:Person)
      WITH like.creationDate AS likeTime, person AS person
        ORDER BY likeTime, message.id
      WITH head(collect({likeTime: likeTime})) AS latestLike, person AS person
      WITH latestLike.likeTime AS likeTime
        ORDER BY likeTime
      RETURN likeTime
      """
    Then the result should be, in order:
      | likeTime |
      | 20160614 |
    And no side effects

  Scenario: [7] Multiple aliasing and backreferencing
    Given any graph
    When executing query:
      """
      CREATE (m {id: 0})
      WITH {first: m.id} AS m
      WITH {second: m.first} AS m
      RETURN m.second
      """
    Then the result should be, in any order:
      | m.second |
      | 0        |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 1 |
