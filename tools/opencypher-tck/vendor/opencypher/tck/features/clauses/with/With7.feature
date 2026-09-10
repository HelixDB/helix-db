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

Feature: With7 - WITH on WITH

  Scenario: [1] A simple pattern with one bound endpoint
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:REL]->(:B)
      """
    When executing query:
      """
      MATCH (a:A)-[r:REL]->(b:B)
      WITH a AS b, b AS tmp, r AS r
      WITH b AS a, r
      LIMIT 1
      MATCH (a)-[r]->(b)
      RETURN a, r, b
      """
    Then the result should be, in any order:
      | a    | r      | b    |
      | (:A) | [:REL] | (:B) |
    And no side effects

  Scenario: [2] Multiple WITHs using a predicate and aggregation
    Given an empty graph
    And having executed:
      """
      CREATE (a {name: 'David'}),
             (b {name: 'Other'}),
             (c {name: 'NotOther'}),
             (d {name: 'NotOther2'}),
             (a)-[:REL]->(b),
             (a)-[:REL]->(c),
             (a)-[:REL]->(d),
             (b)-[:REL]->(),
             (b)-[:REL]->(),
             (c)-[:REL]->(),
             (c)-[:REL]->(),
             (d)-[:REL]->()
      """
    When executing query:
      """
      MATCH (david {name: 'David'})--(otherPerson)-->()
      WITH otherPerson, count(*) AS foaf
      WHERE foaf > 1
      WITH otherPerson
      WHERE otherPerson.name <> 'NotOther'
      RETURN count(*)
      """
    Then the result should be, in any order:
      | count(*) |
      | 1        |
    And no side effects
