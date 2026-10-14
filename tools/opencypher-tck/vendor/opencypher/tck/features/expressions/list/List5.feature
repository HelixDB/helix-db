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

Feature: List5 - List Membership Validation - IN Operator

  Scenario: [1] IN should work with nested list subscripting
    Given any graph
    When executing query:
      """
      WITH [[1, 2, 3]] AS list
      RETURN 3 IN list[0] AS r
      """
    Then the result should be, in any order:
      | r    |
      | true |
    And no side effects

  Scenario: [2] IN should work with nested literal list subscripting
    Given any graph
    When executing query:
      """
      RETURN 3 IN [[1, 2, 3]][0] AS r
      """
    Then the result should be, in any order:
      | r    |
      | true |
    And no side effects

  Scenario: [3] IN should work with list slices
    Given any graph
    When executing query:
      """
      WITH [1, 2, 3] AS list
      RETURN 3 IN list[0..1] AS r
      """
    Then the result should be, in any order:
      | r     |
      | false |
    And no side effects

  Scenario: [4] IN should work with literal list slices
    Given any graph
    When executing query:
      """
      RETURN 3 IN [1, 2, 3][0..1] AS r
      """
    Then the result should be, in any order:
      | r     |
      | false |
    And no side effects

  Scenario: [5] IN should return false when matching a number with a string
    Given any graph
    When executing query:
      """
      RETURN 1 IN ['1', 2] AS res
      """
    Then the result should be, in any order:
      | res   |
      | false |
    And no side effects

  Scenario: [6] IN should return false when matching a number with a string - list version
    Given any graph
    When executing query:
      """
      RETURN [1, 2] IN [1, [1, '2']] AS res
      """
    Then the result should be, in any order:
      | res   |
      | false |
    And no side effects

  Scenario: [7] IN should return false when types of LHS and RHS don't match - singleton list
    Given any graph
    When executing query:
      """
      RETURN [1] IN [1, 2] AS res
      """
    Then the result should be, in any order:
      | res   |
      | false |
    And no side effects

  Scenario: [8] IN should return false when types of LHS and RHS don't match - list
    Given any graph
    When executing query:
      """
      RETURN [1, 2] IN [1, 2] AS res
      """
    Then the result should be, in any order:
      | res   |
      | false |
    And no side effects

  Scenario: [9] IN should return true when types of LHS and RHS match - singleton list
    Given any graph
    When executing query:
      """
      RETURN [1] IN [1, 2, [1]] AS res
      """
    Then the result should be, in any order:
      | res  |
      | true |
    And no side effects

  Scenario: [10] IN should return true when types of LHS and RHS match - list
    Given any graph
    When executing query:
      """
      RETURN [1, 2] IN [1, [1, 2]] AS res
      """
    Then the result should be, in any order:
      | res  |
      | true |
    And no side effects

  Scenario: [11] IN should return false when order of elements in LHS list and RHS list don't match
    Given any graph
    When executing query:
      """
      RETURN [1, 2] IN [1, [2, 1]] AS res
      """
    Then the result should be, in any order:
      | res   |
      | false |
    And no side effects

  Scenario: [12] IN with different length lists should return false
    Given any graph
    When executing query:
      """
      RETURN [1, 2] IN [1, [1, 2, 3]] AS res
      """
    Then the result should be, in any order:
      | res   |
      | false |
    And no side effects

  Scenario: [13] IN should return false when matching a list with a nested list with same elements
    Given any graph
    When executing query:
      """
      RETURN [1, 2] IN [1, [[1, 2]]] AS res
      """
    Then the result should be, in any order:
      | res   |
      | false |
    And no side effects

  Scenario: [14] IN should return true when both LHS and RHS contain nested lists
    Given any graph
    When executing query:
      """
      RETURN [[1, 2], [3, 4]] IN [5, [[1, 2], [3, 4]]] AS res
      """
    Then the result should be, in any order:
      | res  |
      | true |
    And no side effects

  Scenario: [15] IN should return true when both LHS and RHS contain a nested list alongside a scalar element
    Given any graph
    When executing query:
      """
      RETURN [[1, 2], 3] IN [1, [[1, 2], 3]] AS res
      """
    Then the result should be, in any order:
      | res  |
      | true |
    And no side effects

  Scenario: [16] IN should return true when LHS and RHS contain a nested list - singleton version
    Given any graph
    When executing query:
      """
      RETURN [[1]] IN [2, [[1]]] AS res
      """
    Then the result should be, in any order:
      | res  |
      | true |
    And no side effects

  Scenario: [17] IN should return true when LHS and RHS contain a nested list
    Given any graph
    When executing query:
      """
      RETURN [[1, 3]] IN [2, [[1, 3]]] AS res
      """
    Then the result should be, in any order:
      | res  |
      | true |
    And no side effects

  Scenario: [18] IN should return false when LHS contains a nested list and type mismatch on RHS - singleton version
    Given any graph
    When executing query:
      """
      RETURN [[1]] IN [2, [1]] AS res
      """
    Then the result should be, in any order:
      | res   |
      | false |
    And no side effects

  Scenario: [19] IN should return false when LHS contains a nested list and type mismatch on RHS
    Given any graph
    When executing query:
      """
      RETURN [[1, 3]] IN [2, [1, 3]] AS res
      """
    Then the result should be, in any order:
      | res   |
      | false |
    And no side effects

  # IN operator - null

  Scenario: [20] IN should return null if LHS and RHS are null
    Given any graph
    When executing query:
      """
      RETURN null IN [null] AS res
      """
    Then the result should be, in any order:
      | res  |
      | null |
    And no side effects

  Scenario: [21] IN should return null if LHS and RHS are null - list version
    Given any graph
    When executing query:
      """
      RETURN [null] IN [[null]] AS res
      """
    Then the result should be, in any order:
      | res  |
      | null |
    And no side effects

  Scenario: [22] IN should return null when LHS and RHS both ultimately contain null, even if LHS and RHS are of different types (nested list and flat list)
    Given any graph
    When executing query:
      """
      RETURN [null] IN [null] AS res
      """
    Then the result should be, in any order:
      | res  |
      | null |
    And no side effects

  Scenario: [23] IN with different length lists should return false despite nulls
    Given any graph
    When executing query:
      """
      RETURN [1] IN [[1, null]] AS res
      """
    Then the result should be, in any order:
      | res   |
      | false |
    And no side effects

  Scenario: [24] IN should return true if match despite nulls
    Given any graph
    When executing query:
      """
      RETURN 3 IN [1, null, 3] AS res
      """
    Then the result should be, in any order:
      | res  |
      | true |
    And no side effects

  Scenario: [25] IN should return null if comparison with null is required
    Given any graph
    When executing query:
      """
      RETURN 4 IN [1, null, 3] AS res
      """
    Then the result should be, in any order:
      | res  |
      | null |
    And no side effects

  Scenario: [26] IN should return true if correct list found despite other lists having nulls
    Given any graph
    When executing query:
      """
      RETURN [1, 2] IN [[null, 'foo'], [1, 2]] AS res
      """
    Then the result should be, in any order:
      | res  |
      | true |
    And no side effects

  Scenario: [27] IN should return true if correct list found despite null being another element within containing list
    Given any graph
    When executing query:
      """
      RETURN [1, 2] IN [1, [1, 2], null] AS res
      """
    Then the result should be, in any order:
      | res  |
      | true |
    And no side effects

  Scenario: [28] IN should return false if no match can be found, despite nulls
    Given any graph
    When executing query:
      """
      RETURN [1, 2] IN [[null, 'foo']] AS res
      """
    Then the result should be, in any order:
      | res   |
      | false |
    And no side effects

  Scenario: [29] IN should return null if comparison with null is required, list version
    Given any graph
    When executing query:
      """
      RETURN [1, 2] IN [[null, 2]] AS res
      """
    Then the result should be, in any order:
      | res  |
      | null |
    And no side effects

  Scenario: [30] IN should return false if different length lists compared, even if the extra element is null
    Given any graph
    When executing query:
      """
      RETURN [1, 2] IN [1, [1, 2, null]] AS res
      """
    Then the result should be, in any order:
      | res   |
      | false |
    And no side effects

  Scenario: [31] IN should return null when comparing two so-called identical lists where one element is null
    Given any graph
    When executing query:
      """
      RETURN [1, 2, null] IN [1, [1, 2, null]] AS res
      """
    Then the result should be, in any order:
      | res  |
      | null |
    And no side effects

  Scenario: [32] IN should return true with previous null match, list version
    Given any graph
    When executing query:
      """
      RETURN [1, 2] IN [[null, 2], [1, 2]] AS res
      """
    Then the result should be, in any order:
      | res  |
      | true |
    And no side effects

  Scenario: [33] IN should return false if different length lists with nested elements compared, even if the extra element is null
    Given any graph
    When executing query:
      """
      RETURN [[1, 2], [3, 4]] IN [5, [[1, 2], [3, 4], null]] AS res
      """
    Then the result should be, in any order:
      | res   |
      | false |
    And no side effects

  Scenario: [34] IN should return null if comparison with null is required, list version 2
    Given any graph
    When executing query:
      """
      RETURN [1, 2] IN [[null, 2], [1, 3]] AS res
      """
    Then the result should be, in any order:
      | res  |
      | null |
    And no side effects

  # IN operator - empty list

  Scenario: [35] IN should work with an empty list
    Given any graph
    When executing query:
      """
      RETURN [] IN [[]] AS res
      """
    Then the result should be, in any order:
      | res  |
      | true |
    And no side effects

  Scenario: [36] IN should return false for the empty list if the LHS and RHS types differ
    Given any graph
    When executing query:
      """
      RETURN [] IN [] AS res
      """
    Then the result should be, in any order:
      | res   |
      | false |
    And no side effects

  Scenario: [37] IN should work with an empty list in the presence of other list elements: matching
    Given any graph
    When executing query:
      """
      RETURN [] IN [1, []] AS res
      """
    Then the result should be, in any order:
      | res  |
      | true |
    And no side effects

  Scenario: [38] IN should work with an empty list in the presence of other list elements: not matching
    Given any graph
    When executing query:
      """
      RETURN [] IN [1, 2] AS res
      """
    Then the result should be, in any order:
      | res   |
      | false |
    And no side effects

  Scenario: [39] IN should work with an empty list when comparing nested lists
    Given any graph
    When executing query:
      """
      RETURN [[]] IN [1, [[]]] AS res
      """
    Then the result should be, in any order:
      | res  |
      | true |
    And no side effects

  Scenario: [40] IN should return null if comparison with null is required for empty list
    Given any graph
    When executing query:
      """
      RETURN [] IN [1, 2, null] AS res
      """
    Then the result should be, in any order:
      | res  |
      | null |
    And no side effects

  Scenario: [41] IN should return true when LHS and RHS contain nested list with multiple empty lists
    Given any graph
    When executing query:
      """
      RETURN [[], []] IN [1, [[], []]] AS res
      """
    Then the result should be, in any order:
      | res  |
      | true |
    And no side effects

  Scenario Outline: [42] Failing when using IN on a non-list literal
    Given any graph
    When executing query:
      """
      RETURN 1 IN <invalid>
      """
    Then a SyntaxError should be raised at compile time: InvalidArgumentType

    Examples:
      | invalid |
      | true    |
      | 123     |
      | 123.4   |
      | 'foo'   |
      | {x: []} |
