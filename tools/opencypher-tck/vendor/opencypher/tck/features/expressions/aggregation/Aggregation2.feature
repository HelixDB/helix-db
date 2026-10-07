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

Feature: Aggregation2 - Min and Max

  Scenario: [1] `max()` over integers
    Given any graph
    When executing query:
      """
      UNWIND [1, 2, 0, null, -1] AS x
      RETURN max(x)
      """
    Then the result should be, in any order:
      | max(x) |
      | 2      |
    And no side effects

  Scenario: [2] `min()` over integers
    Given any graph
    When executing query:
      """
      UNWIND [1, 2, 0, null, -1] AS x
      RETURN min(x)
      """
    Then the result should be, in any order:
      | min(x) |
      | -1     |
    And no side effects

  Scenario: [3] `max()` over floats
    Given any graph
    When executing query:
      """
      UNWIND [1.0, 2.0, 0.5, null] AS x
      RETURN max(x)
      """
    Then the result should be, in any order:
      | max(x) |
      | 2.0    |
    And no side effects

  Scenario: [4] `min()` over floats
    Given any graph
    When executing query:
      """
      UNWIND [1.0, 2.0, 0.5, null] AS x
      RETURN min(x)
      """
    Then the result should be, in any order:
      | min(x) |
      | 0.5    |
    And no side effects

  Scenario: [5] `max()` over mixed numeric values
    Given any graph
    When executing query:
      """
      UNWIND [1, 2.0, 5, null, 3.2, 0.1] AS x
      RETURN max(x)
      """
    Then the result should be, in any order:
      | max(x) |
      | 5      |
    And no side effects

  Scenario: [6] `min()` over mixed numeric values
    Given any graph
    When executing query:
      """
      UNWIND [1, 2.0, 5, null, 3.2, 0.1] AS x
      RETURN min(x)
      """
    Then the result should be, in any order:
      | min(x) |
      | 0.1    |
    And no side effects

  Scenario: [7] `max()` over strings
    Given any graph
    When executing query:
      """
      UNWIND ['a', 'b', 'B', null, 'abc', 'abc1'] AS i
      RETURN max(i)
      """
    Then the result should be, in any order:
      | max(i) |
      | 'b'    |
    And no side effects

  Scenario: [8] `min()` over strings
    Given any graph
    When executing query:
      """
      UNWIND ['a', 'b', 'B', null, 'abc', 'abc1'] AS i
      RETURN min(i)
      """
    Then the result should be, in any order:
      | min(i) |
      | 'B'    |
    And no side effects

  Scenario: [9] `max()` over list values
    Given any graph
    When executing query:
      """
      UNWIND [[1], [2], [2, 1]] AS x
      RETURN max(x)
      """
    Then the result should be, in any order:
      | max(x) |
      | [2, 1] |
    And no side effects

  Scenario: [10] `min()` over list values
    Given any graph
    When executing query:
      """
      UNWIND [[1], [2], [2, 1]] AS x
      RETURN min(x)
      """
    Then the result should be, in any order:
      | min(x) |
      | [1]    |
    And no side effects

  Scenario: [11] `max()` over mixed values
    Given any graph
    When executing query:
      """
      UNWIND [1, 'a', null, [1, 2], 0.2, 'b'] AS x
      RETURN max(x)
      """
    Then the result should be, in any order:
      | max(x) |
      | 1      |
    And no side effects

  Scenario: [12] `min()` over mixed values
    Given any graph
    When executing query:
      """
      UNWIND [1, 'a', null, [1, 2], 0.2, 'b'] AS x
      RETURN min(x)
      """
    Then the result should be, in any order:
      | min(x) |
      | [1, 2] |
    And no side effects
