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

Feature: Aggregation6 - Percentiles

  Scenario Outline: [1] `percentileDisc()`
    Given an empty graph
    And having executed:
      """
      CREATE ({price: 10.0}),
             ({price: 20.0}),
             ({price: 30.0})
      """
    And parameters are:
      | percentile | <p> |
    When executing query:
      """
      MATCH (n)
      RETURN percentileDisc(n.price, $percentile) AS p
      """
    Then the result should be, in any order:
      | p        |
      | <result> |
    And no side effects

    Examples:
      | p   | result |
      | 0.0 | 10.0   |
      | 0.5 | 20.0   |
      | 1.0 | 30.0   |

  Scenario Outline: [2] `percentileCont()`
    Given an empty graph
    And having executed:
      """
      CREATE ({price: 10.0}),
             ({price: 20.0}),
             ({price: 30.0})
      """
    And parameters are:
      | percentile | <p> |
    When executing query:
      """
      MATCH (n)
      RETURN percentileCont(n.price, $percentile) AS p
      """
    Then the result should be, in any order:
      | p        |
      | <result> |
    And no side effects

    Examples:
      | p   | result |
      | 0.0 | 10.0   |
      | 0.5 | 20.0   |
      | 1.0 | 30.0   |

  Scenario Outline: [3] `percentileCont()` failing on bad arguments
    Given an empty graph
    And having executed:
      """
      CREATE ({price: 10.0})
      """
    And parameters are:
      | param | <percentile> |
    When executing query:
      """
      MATCH (n)
      RETURN percentileCont(n.price, $param)
      """
    Then a ArgumentError should be raised at runtime: NumberOutOfRange

    Examples:
      | percentile |
      | 1000       |
      | -1         |
      | 1.1        |

  Scenario Outline: [4] `percentileDisc()` failing on bad arguments
    Given an empty graph
    And having executed:
      """
      CREATE ({price: 10.0})
      """
    And parameters are:
      | param | <percentile> |
    When executing query:
      """
      MATCH (n)
      RETURN percentileDisc(n.price, $param)
      """
    Then a ArgumentError should be raised at runtime: NumberOutOfRange

    Examples:
      | percentile |
      | 1000       |
      | -1         |
      | 1.1        |

  Scenario: [5] `percentileDisc()` failing in more involved query
    Given an empty graph
    And having executed:
      """
      UNWIND range(0, 10) AS i
      CREATE (s:S)
      WITH s, i
      UNWIND range(0, i) AS j
      CREATE (s)-[:REL]->()
      """
    When executing query:
      """
      MATCH (n:S)
      WITH n, size([(n)-->() | 1]) AS deg
      WHERE deg > 2
      WITH deg
      LIMIT 100
      RETURN percentileDisc(0.90, deg), deg
      """
    Then a ArgumentError should be raised at runtime: NumberOutOfRange
