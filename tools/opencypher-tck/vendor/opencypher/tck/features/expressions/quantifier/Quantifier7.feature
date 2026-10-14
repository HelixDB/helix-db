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

Feature: Quantifier7 - Any quantifier interop

  Scenario Outline: [1] Any quantifier can nest itself and other quantifiers on nested lists
    Given any graph
    When executing query:
      """
      RETURN any(x IN [['abc'], ['abc', 'def']] WHERE <condition>) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    And no side effects

    Examples:
      | condition                      | result |
      | none(y IN x WHERE y = 'def')   | true   |
      | none(y IN x WHERE y = 'abc')   | false  |
      | single(y IN x WHERE y = 'def') | true   |
      | single(y IN x WHERE y = 'ghi') | false  |
      | any(y IN x WHERE y = 'abc')    | true   |
      | any(y IN x WHERE y = 'ghi')    | false  |
      | all(y IN x WHERE y = 'abc')    | true   |
      | all(y IN x WHERE y = 'def')    | false  |

  Scenario Outline: [2] Any quantifier can nest itself and other quantifiers on the same list
    Given any graph
    When executing query:
      """
      WITH [1, 2, 3, 4, 5, 6, 7, 8, 9] AS list
      RETURN any(x IN list WHERE <condition>) AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    And no side effects

    Examples:
      | condition                         | result |
      | none(y IN list WHERE x = y * y)   | true   |
      | none(y IN list WHERE x % y = 0)   | false  |
      | single(y IN list WHERE x = y * y) | true   |
      | single(y IN list WHERE x < y * y) | false  |
      | any(y IN list WHERE x = y)        | true   |
      | any(y IN list WHERE x = 10 * y)   | false  |
      | all(y IN list WHERE x <= y)       | true   |
      | all(y IN list WHERE x < y)        | false  |

  Scenario Outline: [3] Any quantifier is true if the single or the all quantifier is true
    Given any graph
    When executing query:
      """
      RETURN (single(<operands>) OR all(<operands>)) <= any(<operands>) AS result
      """
    # Note that FALSE is less than TRUE, hence A <= B is effectively equivalent to the implication A -> B
    Then the result should be, in any order:
      | result |
      | true   |
    And no side effects

    Examples:
      | operands                                         |
      | x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x = 2     |
      | x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 2 = 0 |
      | x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x % 3 = 0 |
      | x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x < 7     |
      | x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE x >= 3    |

  Scenario Outline: [4] Any quantifier is equal the boolean negative of the none quantifier
    Given any graph
    When executing query:
      """
      RETURN any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE <predicate>) = (NOT none(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE <predicate>)) AS result
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

  Scenario Outline: [5] Any quantifier is equal the boolean negative of the all quantifier on the boolean negative of the predicate
    Given any graph
    When executing query:
      """
      RETURN any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE <predicate>) = (NOT all(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE NOT (<predicate>))) AS result
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

  Scenario Outline: [6] Any quantifier is equal whether the size of the list filtered with same the predicate is grater zero
    Given any graph
    When executing query:
      """
      RETURN any(x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE <predicate>) = (size([x IN [1, 2, 3, 4, 5, 6, 7, 8, 9] WHERE <predicate> | x]) > 0) AS result
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
