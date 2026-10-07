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

Feature: Boolean5 - Interop of logical operations

  Scenario: [1] Disjunction is distributive over conjunction on non-null
    Given any graph
    When executing query:
      """
      UNWIND [true, false] AS a
      UNWIND [true, false] AS b
      UNWIND [true, false] AS c
      RETURN a, b, c, (a OR (b AND c)) = ((a OR b) AND (a OR c)) AS result
      """
    Then the result should be, in any order:
      | a     | b     | c     | result |
      | true  | true  | true  | true   |
      | true  | true  | false | true   |
      | true  | false | true  | true   |
      | true  | false | false | true   |
      | false | true  | true  | true   |
      | false | true  | false | true   |
      | false | false | true  | true   |
      | false | false | false | true   |
    And no side effects

  Scenario: [2] Disjunction is distributive over conjunction on null
    Given any graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [true, false, null] AS b
      UNWIND [true, false, null] AS c
      WITH a, b, c WHERE a IS NULL OR b IS NULL OR c IS NULL
      RETURN a, b, c, (a OR (b AND c)) IS NULL = ((a OR b) AND (a OR c)) IS NULL AS result
      """
    Then the result should be, in any order:
      | a     | b     | c     | result |
      | true  | true  | null  | true   |
      | true  | false | null  | true   |
      | true  | null  | true  | true   |
      | true  | null  | false | true   |
      | true  | null  | null  | true   |
      | false | true  | null  | true   |
      | false | false | null  | true   |
      | false | null  | true  | true   |
      | false | null  | false | true   |
      | false | null  | null  | true   |
      | null  | true  | true  | true   |
      | null  | true  | false | true   |
      | null  | true  | null  | true   |
      | null  | false | true  | true   |
      | null  | false | false | true   |
      | null  | false | null  | true   |
      | null  | null  | true  | true   |
      | null  | null  | false | true   |
      | null  | null  | null  | true   |
    And no side effects

  Scenario: [3] Conjunction is distributive over disjunction on non-null
    Given any graph
    When executing query:
      """
      UNWIND [true, false] AS a
      UNWIND [true, false] AS b
      UNWIND [true, false] AS c
      RETURN a, b, c, (a AND (b OR c)) = ((a AND b) OR (a AND c)) AS result
      """
    Then the result should be, in any order:
      | a     | b     | c     | result |
      | true  | true  | true  | true   |
      | true  | true  | false | true   |
      | true  | false | true  | true   |
      | true  | false | false | true   |
      | false | true  | true  | true   |
      | false | true  | false | true   |
      | false | false | true  | true   |
      | false | false | false | true   |
    And no side effects

  Scenario: [4] Conjunction is distributive over disjunction on null
    Given any graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [true, false, null] AS b
      UNWIND [true, false, null] AS c
      WITH a, b, c WHERE a IS NULL OR b IS NULL OR c IS NULL
      RETURN a, b, c, (a AND (b OR c)) IS NULL = ((a AND b) OR (a AND c)) IS NULL AS result
      """
    Then the result should be, in any order:
      | a     | b     | c     | result |
      | true  | true  | null  | true   |
      | true  | false | null  | true   |
      | true  | null  | true  | true   |
      | true  | null  | false | true   |
      | true  | null  | null  | true   |
      | false | true  | null  | true   |
      | false | false | null  | true   |
      | false | null  | true  | true   |
      | false | null  | false | true   |
      | false | null  | null  | true   |
      | null  | true  | true  | true   |
      | null  | true  | false | true   |
      | null  | true  | null  | true   |
      | null  | false | true  | true   |
      | null  | false | false | true   |
      | null  | false | null  | true   |
      | null  | null  | true  | true   |
      | null  | null  | false | true   |
      | null  | null  | null  | true   |
    And no side effects

  Scenario: [5] Conjunction is distributive over exclusive disjunction on non-null
    Given any graph
    When executing query:
      """
      UNWIND [true, false] AS a
      UNWIND [true, false] AS b
      UNWIND [true, false] AS c
      RETURN a, b, c, (a AND (b XOR c)) = ((a AND b) XOR (a AND c)) AS result
      """
    Then the result should be, in any order:
      | a     | b     | c     | result |
      | true  | true  | true  | true   |
      | true  | true  | false | true   |
      | true  | false | true  | true   |
      | true  | false | false | true   |
      | false | true  | true  | true   |
      | false | true  | false | true   |
      | false | false | true  | true   |
      | false | false | false | true   |
    And no side effects

  Scenario: [6] Conjunction is not distributive over exclusive disjunction on null
    Given any graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [true, false, null] AS b
      UNWIND [true, false, null] AS c
      WITH a, b, c WHERE a IS NULL OR b IS NULL OR c IS NULL
      RETURN a, b, c, (a AND (b XOR c)) IS NULL = ((a AND b) XOR (a AND c)) IS NULL AS result
      """
    Then the result should be, in any order:
      | a     | b     | c     | result |
      | true  | true  | null  | true   |
      | true  | false | null  | true   |
      | true  | null  | true  | true   |
      | true  | null  | false | true   |
      | true  | null  | null  | true   |
      | false | true  | null  | true   |
      | false | false | null  | true   |
      | false | null  | true  | true   |
      | false | null  | false | true   |
      | false | null  | null  | true   |
      | null  | true  | true  | false  |
      | null  | true  | false | true   |
      | null  | true  | null  | true   |
      | null  | false | true  | true   |
      | null  | false | false | true   |
      | null  | false | null  | true   |
      | null  | null  | true  | true   |
      | null  | null  | false | true   |
      | null  | null  | null  | true   |
    And no side effects

  Scenario: [7] De Morgan's law on non-null: the negation of a disjunction is the conjunction of the negations
    Given any graph
    When executing query:
      """
      UNWIND [true, false] AS a
      UNWIND [true, false] AS b
      RETURN a, b, NOT (a OR b) = (NOT (a) AND NOT (b)) AS result
      """
    Then the result should be, in any order:
      | a     | b     | result |
      | true  | true  | true   |
      | true  | false | true   |
      | false | true  | true   |
      | false | false | true   |
    And no side effects

  Scenario: [8] De Morgan's law on non-null: the negation of a conjunction is the disjunction of the negations
    Given any graph
    When executing query:
      """
      UNWIND [true, false] AS a
      UNWIND [true, false] AS b
      RETURN a, b, NOT (a AND b) = (NOT (a) OR NOT (b)) AS result
      """
    Then the result should be, in any order:
      | a     | b     | result |
      | true  | true  | true   |
      | true  | false | true   |
      | false | true  | true   |
      | false | false | true   |
    And no side effects
