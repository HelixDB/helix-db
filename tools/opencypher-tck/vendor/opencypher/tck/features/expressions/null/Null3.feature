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

Feature: Null3 - Null evaluation
# the scenarios of this feature should be redistributed to the features of respective operations

  Scenario: [1] The inverse of a null is a null
    Given any graph
    When executing query:
      """
      RETURN NOT null AS value
      """
    Then the result should be, in any order:
      | value |
      | null  |
    And no side effects

  Scenario: [2] It is unknown - i.e. null - if a null is equal to a null
    Given any graph
    When executing query:
      """
      RETURN null = null AS value
      """
    Then the result should be, in any order:
      | value |
      | null  |
    And no side effects

  Scenario: [3] It is unknown - i.e. null - if a null is not equal to a null
    Given any graph
    When executing query:
      """
      RETURN null <> null AS value
      """
    Then the result should be, in any order:
      | value |
      | null  |
    And no side effects

  Scenario Outline: [4] Using null in IN
    Given any graph
    And parameters are:
      | elt  | <elt>  |
      | coll | <coll> |
    When executing query:
      """
      RETURN $elt IN $coll AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    And no side effects

    Examples:
      | elt  | coll            | result |
      | null | null            | null   |
      | null | [1, 2, 3]       | null   |
      | null | [1, 2, 3, null] | null   |
      | null | []              | false  |
      | 1    | [1, 2, 3, null] | true   |
      | 1    | [null, 1]       | true   |
      | 5    | [1, 2, 3, null] | null   |
