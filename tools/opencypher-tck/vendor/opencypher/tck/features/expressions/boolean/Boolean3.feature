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

Feature: Boolean3 - XOR logical operations

  Scenario: [1] Exclusive disjunction of two truth values
    Given any graph
    When executing query:
      """
      RETURN true XOR true AS tt,
             true XOR false AS tf,
             true XOR null AS tn,
             false XOR true AS ft,
             false XOR false AS ff,
             false XOR null AS fn,
             null XOR true AS nt,
             null XOR false AS nf,
             null XOR null AS nn
      """
    Then the result should be, in any order:
      | tt    | tf   | tn   | ft   | ff    | fn   | nt   | nf   | nn   |
      | false | true | null | true | false | null | null | null | null |
    And no side effects

  Scenario: [2] Exclusive disjunction of three truth values
    Given any graph
    When executing query:
      """
      RETURN true XOR true XOR true AS ttt,
             true XOR true XOR false AS ttf,
             true XOR true XOR null AS ttn,
             true XOR false XOR true AS tft,
             true XOR false XOR false AS tff,
             true XOR false XOR null AS tfn,
             true XOR null XOR true AS tnt,
             true XOR null XOR false AS tnf,
             true XOR null XOR null AS tnn,
             false XOR true XOR true AS ftt,
             false XOR true XOR false AS ftf,
             false XOR true XOR null AS ftn,
             false XOR false XOR true AS fft,
             false XOR false XOR false AS fff,
             false XOR false XOR null AS ffn,
             false XOR null XOR true AS fnt,
             false XOR null XOR false AS fnf,
             false XOR null XOR null AS fnn,
             null XOR true XOR true AS ntt,
             null XOR true XOR false AS ntf,
             null XOR true XOR null AS ntn,
             null XOR false XOR true AS nft,
             null XOR false XOR false AS nff,
             null XOR false XOR null AS nfn,
             null XOR null XOR true AS nnt,
             null XOR null XOR false AS nnf,
             null XOR null XOR null AS nnn
      """
    Then the result should be, in any order:
      | ttt  | ttf   | ttn  | tft   | tff  | tfn  | tnt  | tnf  | tnn  | ftt   | ftf  | ftn  | fft  | fff   | ffn  | fnt  | fnf  | fnn  | ntt  | ntf  | ntn  | nft  | nff  | nfn  | nnt  | nnf  | nnn  |
      | true | false | null | false | true | null | null | null | null | false | true | null | true | false | null | null | null | null | null | null | null | null | null | null | null | null | null |
    And no side effects

  Scenario: [3] Exclusive disjunction of many truth values
    Given any graph
    When executing query:
      """
      RETURN true XOR true XOR true XOR true XOR true XOR true XOR true XOR true XOR true XOR true XOR true AS t,
             true XOR true XOR true XOR false XOR true XOR true XOR true XOR true XOR true XOR true XOR true AS tsf,
             true XOR true XOR true XOR null XOR true XOR true XOR true XOR true XOR true XOR true XOR true AS tsn,
             false XOR false XOR false XOR false XOR false XOR false XOR false XOR false XOR false XOR false XOR false AS f,
             false XOR false XOR false XOR false XOR true XOR false XOR false XOR false XOR false XOR false XOR false AS fst,
             false XOR false XOR false XOR false XOR false XOR false XOR null XOR false XOR false XOR false XOR false AS fsn,
             null XOR null XOR null XOR null XOR null XOR null XOR null XOR null XOR null XOR null XOR null AS n,
             null XOR null XOR null XOR null XOR true XOR null XOR null XOR null XOR null XOR null XOR null AS nst,
             null XOR null XOR null XOR null XOR false XOR null XOR null XOR null XOR null XOR null XOR null AS nsf,
             true XOR false XOR false XOR false XOR true XOR false XOR false XOR true XOR true XOR true XOR false AS m1,
             true XOR true XOR false XOR false XOR true XOR false XOR false XOR true XOR true XOR true XOR false AS m2,
             true XOR true XOR false XOR false XOR true XOR null XOR false XOR true XOR true XOR null XOR false AS m3
      """
    Then the result should be, in any order:
      | t    | tsf   | tsn  | f     | fst  | fsn  | n    | nst  | nsf  | m1    | m2    | m3    |
      | true | false | null | false | true | null | null | null | null | true  | false | null  |
    And no side effects

  Scenario: [4] Exclusive disjunction is commutative on non-null
    Given any graph
    When executing query:
      """
      UNWIND [true, false] AS a
      UNWIND [true, false] AS b
      RETURN a, b, (a XOR b) = (b XOR a) AS result
      """
    Then the result should be, in any order:
      | a     | b     | result |
      | true  | true  | true   |
      | true  | false | true   |
      | false | true  | true   |
      | false | false | true   |
    And no side effects

  Scenario: [5] Exclusive disjunction is commutative on null
    Given any graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [true, false, null] AS b
      WITH a, b WHERE a IS NULL OR b IS NULL
      RETURN a, b, (a XOR b) IS NULL = (b XOR a) IS NULL AS result
      """
    Then the result should be, in any order:
      | a     | b     | result |
      | true  | null  | true   |
      | false | null  | true   |
      | null  | true  | true   |
      | null  | false | true   |
      | null  | null  | true   |
    And no side effects

  Scenario: [6] Exclusive disjunction is associative on non-null
    Given any graph
    When executing query:
      """
      UNWIND [true, false] AS a
      UNWIND [true, false] AS b
      UNWIND [true, false] AS c
      RETURN a, b, c, (a XOR (b XOR c)) = ((a XOR b) XOR c) AS result
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

  Scenario: [7] Exclusive disjunction is associative on null
    Given any graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [true, false, null] AS b
      UNWIND [true, false, null] AS c
      WITH a, b, c WHERE a IS NULL OR b IS NULL OR c IS NULL
      RETURN a, b, c, (a XOR (b XOR c)) IS NULL = ((a XOR b) XOR c) IS NULL AS result
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

  Scenario Outline: [8] Fail on exclusive disjunction of at least one non-booleans
    Given any graph
    When executing query:
      """
      RETURN <a> XOR <b>
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType

    Examples:
      | a       | b       |
      | 123     | true    |
      | 123.4   | false   |
      | 123.4   | null    |
      | 'foo'   | true    |
      | []      | false   |
      | [true]  | false   |
      | [null]  | null    |
      | {}      | true    |
      | {x: []} | true    |
      | false   | 123     |
      | true    | 123.4   |
      | false   | 'foo'   |
      | null    | 'foo'   |
      | true    | []      |
      | true    | [false] |
      | null    | [null]  |
      | false   | {}      |
      | false   | {x: []} |
      | 123     | 'foo'   |
      | 123.4   | 123.4   |
      | 'foo'   | {x: []} |
      | [true]  | [true]  |
      | {x: []} | [123]   |
