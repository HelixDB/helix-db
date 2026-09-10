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

Feature: Remove2 - Remove a Label

  Scenario: [1] Remove a single label from a node with a single label
    Given an empty graph
    And having executed:
      """
      CREATE (:L {num: 42})
      """
    When executing query:
      """
      MATCH (n)
      REMOVE n:L
      RETURN n.num
      """
    Then the result should be, in any order:
      | n.num |
      | 42    |
    And the side effects should be:
      | -labels | 1 |

  Scenario: [2] Remove a single label from a node with two labels
    Given an empty graph
    And having executed:
      """
      CREATE (:Foo:Bar)
      """
    When executing query:
      """
      MATCH (n)
      REMOVE n:Foo
      RETURN labels(n)
      """
    Then the result should be, in any order:
      | labels(n) |
      | ['Bar']   |
    And the side effects should be:
      | -labels | 1 |

  Scenario: [3] Remove two labels from a node with three labels
    Given an empty graph
    And having executed:
      """
      CREATE (:L1:L2:L3 {num: 42})
      """
    When executing query:
      """
      MATCH (n)
      REMOVE n:L1:L3
      RETURN labels(n)
      """
    Then the result should be, in any order:
      | labels(n) |
      | ['L2']    |
    And the side effects should be:
      | -labels | 2 |

  Scenario: [4] Remove a non-existent node label
    Given an empty graph
    And having executed:
      """
      CREATE (:Foo)
      """
    When executing query:
      """
      MATCH (n)
      REMOVE n:Bar
      RETURN labels(n)
      """
    Then the result should be, in any order:
      | labels(n) |
      | ['Foo']   |
    And no side effects

  Scenario: [5] Ignore null when removing a node label
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH (a:DoesNotExist)
      REMOVE a:L
      RETURN a
      """
    Then the result should be, in any order:
      | a    |
      | null |
    And no side effects
