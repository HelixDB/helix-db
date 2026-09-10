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

Feature: Boolean1 - And logical operations

  Scenario: [1] Conjunction of two truth values
    Given any graph
    When executing query:
      """
      RETURN true AND true AS tt,
             true AND false AS tf,
             true AND null AS tn,
             false AND true AS ft,
             false AND false AS ff,
             false AND null AS fn,
             null AND true AS nt,
             null AND false AS nf,
             null AND null AS nn
      """
    Then the result should be, in any order:
      | tt   | tf    | tn   | ft    | ff    | fn    | nt   | nf    | nn   |
      | true | false | null | false | false | false | null | false | null |
    And no side effects

  Scenario: [2] Conjunction of three truth values
    Given any graph
    When executing query:
      """
      RETURN true AND true AND true AS ttt,
             true AND true AND false AS ttf,
             true AND true AND null AS ttn,
             true AND false AND true AS tft,
             true AND false AND false AS tff,
             true AND false AND null AS tfn,
             true AND null AND true AS tnt,
             true AND null AND false AS tnf,
             true AND null AND null AS tnn,
             false AND true AND true AS ftt,
             false AND true AND false AS ftf,
             false AND true AND null AS ftn,
             false AND false AND true AS fft,
             false AND false AND false AS fff,
             false AND false AND null AS ffn,
             false AND null AND true AS fnt,
             false AND null AND false AS fnf,
             false AND null AND null AS fnn,
             null AND true AND true AS ntt,
             null AND true AND false AS ntf,
             null AND true AND null AS ntn,
             null AND false AND true AS nft,
             null AND false AND false AS nff,
             null AND false AND null AS nfn,
             null AND null AND true AS nnt,
             null AND null AND false AS nnf,
             null AND null AND null AS nnn
      """
    Then the result should be, in any order:
      | ttt  | ttf   | ttn  | tft   | tff   | tfn   | tnt  | tnf   | tnn  | ftt   | ftf   | ftn   | fft   | fff   | ffn   | fnt   | fnf   | fnn   | ntt  | ntf   | ntn  | nft   | nff   | nfn   | nnt  | nnf   | nnn  |
      | true | false | null | false | false | false | null | false | null | false | false | false | false | false | false | false | false | false | null | false | null | false | false | false | null | false | null |
    And no side effects

  Scenario: [3] Conjunction of many truth values
    Given any graph
    When executing query:
      """
      RETURN true AND true AND true AND true AND true AND true AND true AND true AND true AND true AND true AS t,
             true AND true AND true AND false AND true AND true AND true AND true AND true AND true AND true AS tsf,
             true AND true AND true AND null AND true AND true AND true AND true AND true AND true AND true AS tsn,
             false AND false AND false AND false AND false AND false AND false AND false AND false AND false AND false AS f,
             false AND false AND false AND false AND true AND false AND false AND false AND false AND false AND false AS fst,
             false AND false AND false AND false AND false AND false AND null AND false AND false AND false AND false AS fsn,
             null AND null AND null AND null AND null AND null AND null AND null AND null AND null AND null AS n,
             null AND null AND null AND null AND true AND null AND null AND null AND null AND null AND null AS nst,
             null AND null AND null AND null AND false AND null AND null AND null AND null AND null AND null AS nsf,
             true AND false AND false AND false AND true AND false AND false AND true AND true AND true AND false AS m1,
             true AND true AND false AND false AND true AND false AND false AND true AND true AND true AND false AS m2,
             true AND true AND false AND false AND true AND null AND false AND true AND true AND null AND false AS m3
      """
    Then the result should be, in any order:
      | t    | tsf   | tsn  | f     | fst   | fsn   | n    | nst  | nsf   | m1    | m2    | m3    |
      | true | false | null | false | false | false | null | null | false | false | false | false |
    And no side effects

  Scenario: [4] Conjunction is commutative on non-null
    Given any graph
    When executing query:
      """
      UNWIND [true, false] AS a
      UNWIND [true, false] AS b
      RETURN a, b, (a AND b) = (b AND a) AS result
      """
    Then the result should be, in any order:
      | a     | b     | result |
      | true  | true  | true   |
      | true  | false | true   |
      | false | true  | true   |
      | false | false | true   |
    And no side effects

  Scenario: [5] Conjunction is commutative on null
    Given any graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [true, false, null] AS b
      WITH a, b WHERE a IS NULL OR b IS NULL
      RETURN a, b, (a AND b) IS NULL = (b AND a) IS NULL AS result
      """
    Then the result should be, in any order:
      | a     | b     | result |
      | true  | null  | true   |
      | false | null  | true   |
      | null  | true  | true   |
      | null  | false | true   |
      | null  | null  | true   |
    And no side effects

  Scenario: [6] Conjunction is associative on non-null
    Given any graph
    When executing query:
      """
      UNWIND [true, false] AS a
      UNWIND [true, false] AS b
      UNWIND [true, false] AS c
      RETURN a, b, c, (a AND (b AND c)) = ((a AND b) AND c) AS result
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

  Scenario: [7] Conjunction is associative on null
    Given any graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [true, false, null] AS b
      UNWIND [true, false, null] AS c
      WITH a, b, c WHERE a IS NULL OR b IS NULL OR c IS NULL
      RETURN a, b, c, (a AND (b AND c)) IS NULL = ((a AND b) AND c) IS NULL AS result
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

  Scenario Outline: [8] Fail on conjunction of at least one non-booleans
    Given any graph
    When executing query:
      """
      RETURN <a> AND <b>
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
