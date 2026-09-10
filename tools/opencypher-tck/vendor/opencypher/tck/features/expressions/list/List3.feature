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

Feature: List3 - List Equality

  Scenario: [1] Equality between list and literal should return false
    Given any graph
    When executing query:
      """
      RETURN [1, 2] = 'foo' AS res
      """
    Then the result should be, in any order:
      | res   |
      | false |
    And no side effects

  Scenario: [2] Equality of lists of different length should return false despite nulls
    Given any graph
    When executing query:
      """
      RETURN [1] = [1, null] AS res
      """
    Then the result should be, in any order:
      | res   |
      | false |
    And no side effects

  Scenario: [3] Equality between different lists with null should return false
    Given any graph
    When executing query:
      """
      RETURN [1, 2] = [null, 'foo'] AS res
      """
    Then the result should be, in any order:
      | res   |
      | false |
    And no side effects

  Scenario: [4] Equality between almost equal lists with null should return null
    Given any graph
    When executing query:
      """
      RETURN [1, 2] = [null, 2] AS res
      """
    Then the result should be, in any order:
      | res  |
      | null |
    And no side effects

  Scenario: [5] Equality of nested lists of different length should return false despite nulls
    Given any graph
    When executing query:
      """
      RETURN [[1]] = [[1], [null]] AS res
      """
    Then the result should be, in any order:
      | res   |
      | false |
    And no side effects

  Scenario: [6] Equality between different nested lists with null should return false
    Given any graph
    When executing query:
      """
      RETURN [[1, 2], [1, 3]] = [[1, 2], [null, 'foo']] AS res
      """
    Then the result should be, in any order:
      | res   |
      | false |
    And no side effects

  Scenario: [7] Equality between almost equal nested lists with null should return null
    Given any graph
    When executing query:
      """
      RETURN [[1, 2], ['foo', 'bar']] = [[1, 2], [null, 'bar']] AS res
      """
    Then the result should be, in any order:
      | res  |
      | null |
    And no side effects
