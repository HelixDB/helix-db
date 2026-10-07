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

Feature: Quantifier8 - All quantifier interop

  Scenario Outline: [1] All quantifier can nest itself and other quantifiers on nested lists
    Given any graph
    When executing query:
      """
      RETURN all(x IN [['abc'], ['abc', 'def']] WHERE <condition>) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    And no side effects

    Examples:
      | condition                      | result |
      | none(y IN x WHERE y = 'ghi')   | true   |
      | none(y IN x WHERE y = 'def')   | false  |
      | single(y IN x WHERE y = 'abc') | true   |
      | single(y IN x WHERE y = 'ghi') | false  |
      | any(y IN x WHERE y = 'abc')    | true   |
      | any(y IN x WHERE y = 'ghi')    | false  |
      | all(y IN x WHERE y <> 'ghi')   | true   |
      | all(y IN x WHERE y = 'abc')    | false  |

  Scenario Outline: [2] All quantifier can nest itself and other quantifiers on the same list
    Given any graph
    When executing query:
      """
      WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
      RETURN all(x IN list WHERE <condition>) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    And no side effects

    Examples:
      | condition                            | result |
      | none(y IN list WHERE x = 10 * y)     | true   |
      | none(y IN list WHERE x = y)          | false  |
      | single(y IN list WHERE x = y)        | true   |
      | single(y IN list WHERE x < y)        | false  |
      | any(y IN list WHERE x % y = 0)       | true   |
      | any(y IN list WHERE x < y)           | false  |
      | all(y IN list WHERE abs(x - y) < 10) | true   |
      | all(y IN list WHERE x < y + 7)       | false  |

  Scenario Outline: [3] All quantifier is equal the none quantifier on the boolean negative of the predicate
    Given any graph
    When executing query:
      """
      RETURN all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE <predicate>) = none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE NOT (<predicate>)) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

    Examples:
      | predicate |
      | x = 2     |
      | x % 2 = 0 |
      | x % 3 = 0 |
      | x < 7     |
      | x >= 3    |

  Scenario Outline: [4] All quantifier is equal the boolean negative of the any quantifier on the boolean negative of the predicate
    Given any graph
    When executing query:
      """
      RETURN all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE <predicate>) = (NOT any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE NOT (<predicate>))) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

    Examples:
      | predicate |
      | x = 2     |
      | x % 2 = 0 |
      | x % 3 = 0 |
      | x < 7     |
      | x >= 3    |

  Scenario Outline: [5] All quantifier is equal whether the size of the list filtered with same the predicate is equal the size of the unfiltered list
    Given any graph
    When executing query:
      """
      RETURN all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE <predicate>) = (size([x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE <predicate> | x]) = size([1, 2, 3, 4, 5, 6, 7, 8, 9])) AS result
      """
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

    Examples:
      | predicate |
      | x = 2     |
      | x % 2 = 0 |
      | x % 3 = 0 |
      | x < 7     |
      | x >= 3    |
