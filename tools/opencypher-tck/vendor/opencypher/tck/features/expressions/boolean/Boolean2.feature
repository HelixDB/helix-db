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

Feature: Boolean2 - OR logical operations

  Scenario: [1] Disjunction of two truth values
    Given any graph
    When executing query:
      """
      RETURN true OR true AS tt,
             true OR false AS tf,
             true OR null AS tn,
             false OR true AS ft,
             false OR false AS ff,
             false OR null AS fn,
             null OR true AS nt,
             null OR false AS nf,
             null OR null AS nn
      """
    Then the result should be, in any order:
      | tt   | tf   | tn   | ft   | ff    | fn   | nt   | nf   | nn   |
      | true | true | true | true | false | null | true | null | null |
    And no side effects

  Scenario: [2] Disjunction of three truth values
    Given any graph
    When executing query:
      """
      RETURN true OR true OR true AS ttt,
             true OR true OR false AS ttf,
             true OR true OR null AS ttn,
             true OR false OR true AS tft,
             true OR false OR false AS tff,
             true OR false OR null AS tfn,
             true OR null OR true AS tnt,
             true OR null OR false AS tnf,
             true OR null OR null AS tnn,
             false OR true OR true AS ftt,
             false OR true OR false AS ftf,
             false OR true OR null AS ftn,
             false OR false OR true AS fft,
             false OR false OR false AS fff,
             false OR false OR null AS ffn,
             false OR null OR true AS fnt,
             false OR null OR false AS fnf,
             false OR null OR null AS fnn,
             null OR true OR true AS ntt,
             null OR true OR false AS ntf,
             null OR true OR null AS ntn,
             null OR false OR true AS nft,
             null OR false OR false AS nff,
             null OR false OR null AS nfn,
             null OR null OR true AS nnt,
             null OR null OR false AS nnf,
             null OR null OR null AS nnn
      """
    Then the result should be, in any order:
      | ttt  | ttf  | ttn  | tft  | tff  | tfn  | tnt  | tnf  | tnn  | ftt  | ftf  | ftn  | fft  | fff   | ffn  | fnt  | fnf  | fnn  | ntt  | ntf  | ntn  | nft  | nff  | nfn  | nnt  | nnf  | nnn  |
      | true | true | true | true | true | true | true | true | true | true | true | true | true | false | null | true | null | null | true | true | true | true | null | null | true | null | null |
    And no side effects

  Scenario: [3] Disjunction of many truth values
    Given any graph
    When executing query:
      """
      RETURN true OR true OR true OR true OR true OR true OR true OR true OR true OR true OR true AS t,
             true OR true OR true OR false OR true OR true OR true OR true OR true OR true OR true AS tsf,
             true OR true OR true OR null OR true OR true OR true OR true OR true OR true OR true AS tsn,
             false OR false OR false OR false OR false OR false OR false OR false OR false OR false OR false AS f,
             false OR false OR false OR false OR true OR false OR false OR false OR false OR false OR false AS fst,
             false OR false OR false OR false OR false OR false OR null OR false OR false OR false OR false AS fsn,
             null OR null OR null OR null OR null OR null OR null OR null OR null OR null OR null AS n,
             null OR null OR null OR null OR true OR null OR null OR null OR null OR null OR null AS nst,
             null OR null OR null OR null OR false OR null OR null OR null OR null OR null OR null AS nsf,
             true OR false OR false OR false OR true OR false OR false OR true OR true OR true OR false AS m1,
             true OR true OR false OR false OR true OR false OR false OR true OR true OR true OR false AS m2,
             true OR true OR false OR false OR true OR null OR false OR true OR true OR null OR false AS m3
      """
    Then the result should be, in any order:
      | t    | tsf  | tsn  | f     | fst  | fsn  | n    | nst  | nsf  | m1    | m2   | m3   |
      | true | true | true | false | true | null | null | true | null | true  | true | true |
    And no side effects

  Scenario: [4] Disjunction is commutative on non-null
    Given any graph
    When executing query:
      """
      UNWIND [true, false] AS a
      UNWIND [true, false] AS b
      RETURN a, b, (a OR b) = (b OR a) AS result
      """
    Then the result should be, in any order:
      | a     | b     | result |
      | true  | true  | true   |
      | true  | false | true   |
      | false | true  | true   |
      | false | false | true   |
    And no side effects

  Scenario: [5] Disjunction is commutative on null
    Given any graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [true, false, null] AS b
      WITH a, b WHERE a IS NULL OR b IS NULL
      RETURN a, b, (a OR b) IS NULL = (b OR a) IS NULL AS result
      """
    Then the result should be, in any order:
      | a     | b     | result |
      | true  | null  | true   |
      | false | null  | true   |
      | null  | true  | true   |
      | null  | false | true   |
      | null  | null  | true   |
    And no side effects

  Scenario: [6] Disjunction is associative on non-null
    Given any graph
    When executing query:
      """
      UNWIND [true, false] AS a
      UNWIND [true, false] AS b
      UNWIND [true, false] AS c
      RETURN a, b, c, (a OR (b OR c)) = ((a OR b) OR c) AS result
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

  Scenario: [7] Disjunction is associative on null
    Given any graph
    When executing query:
      """
      UNWIND [true, false, null] AS a
      UNWIND [true, false, null] AS b
      UNWIND [true, false, null] AS c
      WITH a, b, c WHERE a IS NULL OR b IS NULL OR c IS NULL
      RETURN a, b, c, (a OR (b OR c)) IS NULL = ((a OR b) OR c) IS NULL AS result
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

  Scenario Outline: [8] Fail on disjunction of at least one non-booleans
    Given any graph
    When executing query:
      """
      RETURN <a> OR <b>
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
