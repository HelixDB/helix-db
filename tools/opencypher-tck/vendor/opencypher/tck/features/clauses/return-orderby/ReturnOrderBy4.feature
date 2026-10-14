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

Feature: ReturnOrderBy4 - Order by in combination with projection

  Scenario: [1] ORDER BY of a column introduced in RETURN should return salient results in ascending order
    Given an empty graph
    When executing query:
      """
      WITH [0, 1] AS prows, [[2], [3, 4]] AS qrows
      UNWIND prows AS p
      UNWIND qrows[p] AS q
      WITH p, count(q) AS rng
      RETURN p
      ORDER BY rng
      """
    Then the result should be, in order:
      | p |
      | 0 |
      | 1 |
    And no side effects

  Scenario: [2] Handle projections with ORDER BY
    Given an empty graph
    And having executed:
      """
      CREATE (c1:Crew {name: 'Neo', rank: 1}),
        (c2:Crew {name: 'Neo', rank: 2}),
        (c3:Crew {name: 'Neo', rank: 3}),
        (c4:Crew {name: 'Neo', rank: 4}),
        (c5:Crew {name: 'Neo', rank: 5})
      """
    When executing query:
      """
      MATCH (c:Crew {name: 'Neo'})
      WITH c, 0 AS relevance
      RETURN c.rank AS rank
      ORDER BY relevance, c.rank
      """
    Then the result should be, in order:
      | rank |
      | 1    |
      | 2    |
      | 3    |
      | 4    |
      | 5    |
    And no side effects
