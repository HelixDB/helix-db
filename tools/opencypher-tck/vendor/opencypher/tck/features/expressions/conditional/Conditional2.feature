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

Feature: Conditional2 - Case Expression

  Scenario Outline: [1] Simple cases over integers
    Given an empty graph
    When executing query:
      """
      RETURN CASE <value>
          WHEN -10 THEN 'minus ten'
          WHEN 0 THEN 'zero'
          WHEN 1 THEN 'one'
          WHEN 5 THEN 'five'
          WHEN 10 THEN 'ten'
          WHEN 3000 THEN 'three thousand'
          ELSE 'something else'
        END AS result
      """
    Then the result should be, in any order:
      | result   |
      | <result> |
    And no side effects

    Examples:
      | value | result           |
      | -10   | 'minus ten'      |
      | 0     | 'zero'           |
      | 1     | 'one'            |
      | 5     | 'five'           |
      | 10    | 'ten'            |
      | 3000  | 'three thousand' |
      | -30   | 'something else' |
      | 3     | 'something else' |
      | 3001  | 'something else' |
      | '0'   | 'something else' |
      | true  | 'something else' |
      | 10.1  | 'something else' |

