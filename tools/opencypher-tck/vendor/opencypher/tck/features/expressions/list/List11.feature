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

Feature: List11 - Create a list from a range

  Scenario Outline: [1] Create list from `range()` with default step
    Given any graph
    When executing query:
      """
      RETURN range(<start>, <end>) AS list
      """
    Then the result should be, in any order:
      | list   |
      | <list> |
    And no side effects

    Examples:
      | start | end   | list                                         |
      | -1236 | -1234 | [-1236, -1235, -1234]                        |
      | -1234 | -1234 | [-1234]                                      |
      | -10   | -3    | [-10, -9, -8, -7, -6, -5, -4, -3]            |
      | -10   | 0     | [-10, -9, -8, -7, -6, -5, -4, -3, -2, -1, 0] |
      | -1    | 0     | [-1, 0]                                      |
      | 0     | -123  | []                                           |
      | 0     | -1    | []                                           |
      | -1    | 1     | [-1, 0, 1]                                   |
      | 0     | 0     | [0]                                          |
      | 0     | 1     | [0, 1]                                       |
      | 0     | 10    | [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]           |
      | 6     | 10    | [6, 7, 8, 9, 10]                             |
      | 1234  | 1234  | [1234]                                       |
      | 1234  | 1236  | [1234, 1235, 1236]                           |

  Scenario Outline: [2] Create list from `range()` with explicitly given step
    Given any graph
    When executing query:
      """
      RETURN range(<start>, <end>, <step>) AS list
      """
    Then the result should be, in any order:
      | list   |
      | <list> |
    And no side effects

    Examples:
      | start | end   | step  | list                                              |
      | 1381  | -3412 | -1298 | [1381, 83, -1215, -2513]                          |
      | 0     | -2000 | -1298 | [0, -1298]                                        |
      | 10    | -10   | -3    | [10, 7, 4, 1, -2, -5, -8]                         |
      | 0     | -10   | -3    | [0, -3, -6, -9]                                   |
      | 0     | -20   | -2    | [0, -2, -4, -6, -8, -10, -12, -14, -16, -18, -20] |
      | 0     | -10   | -1    | [0, -1, -2, -3, -4, -5, -6, -7, -8, -9, -10]      |
      | 0     | -1    | -1    | [0, -1]                                           |
      | -1236 | -1234 | 1     | [-1236, -1235, -1234]                             |
      | -10   | 0     | 1     | [-10, -9, -8, -7, -6, -5, -4, -3, -2, -1, 0]      |
      | -1    | 0     | 1     | [-1, 0]                                           |
      | 0     | 1     | -123  | []                                                |
      | 0     | 1     | -1    | []                                                |
      | 0     | -123  | 1     | []                                                |
      | 0     | -1    | 1     | []                                                |
      | 0     | 0     | -1    | [0]                                               |
      | 0     | 0     | 1     | [0]                                               |
      | 0     | 1     | 2     | [0]                                               |
      | 0     | 1     | 1     | [0, 1]                                            |
      | 0     | 10    | 1     | [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]                |
      | 6     | 10    | 1     | [6, 7, 8, 9, 10]                                  |
      | 1234  | 1234  | 1     | [1234]                                            |
      | 1234  | 1236  | 1     | [1234, 1235, 1236]                                |
      | -10   | 0     | 3     | [-10, -7, -4, -1]                                 |
      | -10   | 10    | 3     | [-10, -7, -4, -1, 2, 5, 8]                        |
      | -2000 | 0     | 1298  | [-2000, -702]                                     |
      | -3412 | 1381  | 1298  | [-3412, -2114, -816, 482]                         |

  Scenario: [3] Create an empty list if range direction and step direction are inconsistent
    Given any graph
    When executing query:
      """
      WITH 0 AS start, [1, 2, 500, 1000, 1500] AS stopList, [-1000, -3, -2, -1, 1, 2, 3, 1000] AS stepList
      UNWIND stopList AS stop
      UNWIND stepList AS step
      WITH start, stop, step, range(start, stop, step) AS list
      WITH start, stop, step, list, sign(stop-start) <> sign(step) AS empty
      RETURN ALL(ok IN collect((size(list) = 0) = empty) WHERE ok) AS okay
      """
    Then the result should be, in any order:
      | okay |
      | true |
    And no side effects

  Scenario Outline: [4] Fail on invalid arguments for `range()`
    Given any graph
    When executing query:
      """
      RETURN range(2, 8, 0)
      """
    Then a ArgumentError should be raised at runtime: NumberOutOfRange

    Examples:
      | start | end  | step |
      | 0     | 0    | 0    |
      | 2     | 8    | 0    |
      | -2    | 8    | 0    |
      | 2     | -8   | 0    |

  Scenario Outline: [5] Fail on invalid argument types for `range()`
    Given any graph
    When executing query:
      """
      RETURN range(<start>, <end>, <step>)
      """
    Then a ArgumentError should be raised at runtime: InvalidArgumentType

    Examples:
      | start      | end      | step      |
      | true       | 1        | 1         |
      | 0          | true     | 1         |
      | 0          | 1        | true      |
      | -1.1       | 1        | 1         |
      | -0.0       | 1        | 1         |
      | 0.0        | 1        | 1         |
      | 1.1        | 1        | 1         |
      | 0          | -1.1     | 1         |
      | 0          | -0.0     | 1         |
      | 0          | 0.0      | 1         |
      | 0          | 1.1      | 1         |
      | 0          | 1        | -1.1      |
      | 0          | 1        | 1.1       |
      | 'xyz'      | 1        | 1         |
      | 0          | 'xyz'    | 1         |
      | 0          | 1        | 'xyz'     |
      | [0]        | 1        | 1         |
      | 0          | [1]      | 1         |
      | 0          | 1        | [1]       |
      | {start: 0} | 1        | 1         |
      | 0          | {end: 1} | 1         |
      | 0          | 1        | {step: 1} |
