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

Feature: WithWhere2 - Filter multiple variables

  Scenario: [1] Filter nodes with conjunctive two-part property predicate on multi variables with multiple bindings
    Given an empty graph
    And having executed:
      """
      CREATE (a:A), (b:B {id: 1}), (c:C {id: 2}), (d:D)
      CREATE (a)-[:T]->(b),
             (a)-[:T]->(c),
             (a)-[:T]->(d),
             (b)-[:T]->(c),
             (b)-[:T]->(d),
             (c)-[:T]->(d)
      """
    When executing query:
      """
      MATCH (a)--(b)--(c)--(d)--(a), (b)--(d)
      WITH a, c, d
      WHERE a.id = 1
        AND c.id = 2
      RETURN d
      """
    Then the result should be, in any order:
      | d    |
      | (:A) |
      | (:D) |
    And no side effects

  Scenario: [2] Filter node with conjunctive multi-part property predicates on multi variables with multiple bindings
    Given an empty graph
    And having executed:
      """
      CREATE (advertiser {name: 'advertiser1', id: 0}),
             (thing {name: 'Color', id: 1}),
             (red {name: 'red'}),
             (p1 {name: 'product1'}),
             (p2 {name: 'product4'})
      CREATE (advertiser)-[:ADV_HAS_PRODUCT]->(p1),
             (advertiser)-[:ADV_HAS_PRODUCT]->(p2),
             (thing)-[:AA_HAS_VALUE]->(red),
             (p1)-[:AP_HAS_VALUE]->(red),
             (p2)-[:AP_HAS_VALUE]->(red)
      """
    And parameters are:
      | 1 | 0 |
      | 2 | 1 |
    When executing query:
      """
      MATCH (advertiser)-[:ADV_HAS_PRODUCT]->(out)-[:AP_HAS_VALUE]->(red)<-[:AA_HAS_VALUE]-(a)
      WITH a, advertiser, red, out
      WHERE advertiser.id = $1
        AND a.id = $2
        AND red.name = 'red'
        AND out.name = 'product1'
      RETURN out.name
      """
    Then the result should be, in any order:
      | out.name   |
      | 'product1' |
    And no side effects
