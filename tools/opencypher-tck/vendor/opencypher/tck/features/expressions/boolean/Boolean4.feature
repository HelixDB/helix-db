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

Feature: Boolean4 - NOT logical operations

  Scenario: [1] Logical negation of truth values
    Given any graph
    When executing query:
      """
      RETURN NOT true AS nt, NOT false AS nf, NOT null AS nn
      """
    Then the result should be, in any order:
      | nt    | nf   | nn   |
      | false | true | null |
    And no side effects

  Scenario: [2] Double logical negation of truth values
    Given any graph
    When executing query:
      """
      RETURN NOT NOT true AS nnt, NOT NOT false AS nnf, NOT NOT null AS nnn
      """
    Then the result should be, in any order:
      | nnt  | nnf   | nnn  |
      | true | false | null |
    And no side effects

  Scenario: [3] NOT and false
    Given an empty graph
    And having executed:
      """
      CREATE ({name: 'a'})
      """
    When executing query:
      """
      MATCH (n)
      WHERE NOT(n.name = 'apa' AND false)
      RETURN n
      """
    Then the result should be, in any order:
      | n             |
      | ({name: 'a'}) |
    And no side effects

  Scenario Outline: [4] Fail when using NOT on a non-boolean literal
    Given any graph
    When executing query:
      """
      RETURN NOT <literal>
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType

    Examples:
      | literal          |
      | 0                |
      | 1                |
      | 123              |
      | 123.4            |
      | ''               |
      | 'false'          |
      | 'true'           |
      | 'foo'            |
      | []               |
      | [null]           |
      | [true]           |
      | [false]          |
      | [true, false]    |
      | [false, true]    |
      | [0]              |
      | [1]              |
      | [1, 2, 3]        |
      | [0.0]            |
      | [1.0]            |
      | [1.0, 2.1]       |
      | ['']             |
      | ['', '']         |
      | ['true']         |
      | ['false']        |
      | ['a', 'b']       |
      | {}               |
      | {``: null}       |
      | {a: null}        |
      | {``: true}       |
      | {``: false}      |
      | {true: true}     |
      | {false: false}   |
      | {bool: true}     |
      | {bool: false}    |
      | {``: 0}          |
      | {``: 1}          |
      | {a: 0}           |
      | {a: 1}           |
      | {a: 1, b: 2}     |
      | {``: 0.0}        |
      | {``: 1.0}        |
      | {a: 0.0}         |
      | {a: 1.0}         |
      | {a: 1.0, b: 2.1} |
      | {``: ''}         |
      | {a: ''}          |
      | {a: 'a'}         |
      | {a: 'a', b: 'b'} |
      | {a: 12, b: true} |
