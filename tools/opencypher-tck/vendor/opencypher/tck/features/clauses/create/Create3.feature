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

Feature: Create3 - Interoperation with other clauses

  Scenario: [1] MATCH-CREATE
    Given an empty graph
    And having executed:
      """
      CREATE (), ()
      """
    When executing query:
      """
      MATCH ()
      CREATE ()
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 2 |

  Scenario: [2] WITH-CREATE
    Given an empty graph
    And having executed:
      """
      CREATE (), ()
      """
    When executing query:
      """
      MATCH ()
      CREATE ()
      WITH *
      CREATE ()
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes | 4 |

  Scenario: [3] MATCH-CREATE-WITH-CREATE
    Given an empty graph
    And having executed:
      """
      CREATE (), ()
      """
    When executing query:
      """
      MATCH ()
      CREATE ()
      WITH *
      MATCH ()
      CREATE ()
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 10 |

  Scenario: [4] MATCH-CREATE: Newly-created nodes not visible to preceding MATCH
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      MATCH ()
      CREATE ()
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 1 |

  Scenario: [5] WITH-CREATE: Nodes are not created when aliases are applied to variable names
    Given an empty graph
    And having executed:
      """
      CREATE ({num: 1})
      """
    When executing query:
      """
      MATCH (n)
      MATCH (m)
      WITH n AS a, m AS b
      CREATE (a)-[:T]->(b)
      RETURN a, b
      """
    Then the result should be, in any order:
      | a          | b          |
      | ({num: 1}) | ({num: 1}) |
    And the side effects should be:
      | +relationships | 1 |

  Scenario: [6] WITH-CREATE: Only a single node is created when an alias is applied to a variable name
    Given an empty graph
    And having executed:
      """
      CREATE (:X)
      """
    When executing query:
      """
      MATCH (n)
      WITH n AS a
      CREATE (a)-[:T]->()
      RETURN a
      """
    Then the result should be, in any order:
      | a    |
      | (:X) |
    And the side effects should be:
      | +nodes         | 1 |
      | +relationships | 1 |

  Scenario: [7] WITH-CREATE: Nodes are not created when aliases are applied to variable names multiple times
    Given an empty graph
    And having executed:
      """
      CREATE ({name: 'A'})
      """
    When executing query:
      """
      MATCH (n)
      MATCH (m)
      WITH n AS a, m AS b
      CREATE (a)-[:T]->(b)
      WITH a AS x, b AS y
      CREATE (x)-[:T]->(y)
      RETURN x, y
      """
    Then the result should be, in any order:
      | x             | y             |
      | ({name: 'A'}) | ({name: 'A'}) |
    And the side effects should be:
      | +relationships | 2 |

  Scenario: [8] WITH-CREATE: Only a single node is created when an alias is applied to a variable name multiple times
    Given an empty graph
    And having executed:
      """
      CREATE ({num: 5})
      """
    When executing query:
      """
      MATCH (n)
      WITH n AS a
      CREATE (a)-[:T]->()
      WITH a AS x
      CREATE (x)-[:T]->()
      RETURN x
      """
    Then the result should be, in any order:
      | x          |
      | ({num: 5}) |
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 2 |

  Scenario: [9] WITH-CREATE: A bound node should be recognized after projection with WITH + WITH
    Given any graph
    When executing query:
      """
      CREATE (a)
      WITH a
      WITH *
      CREATE (b)
      CREATE (a)<-[:T]-(b)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |

  Scenario: [10] WITH-UNWIND-CREATE: A bound node should be recognized after projection with WITH + UNWIND
    Given any graph
    When executing query:
      """
      CREATE (a)
      WITH a
      UNWIND [0] AS i
      CREATE (b)
      CREATE (a)<-[:T]-(b)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |

  Scenario: [11] WITH-MERGE-CREATE: A bound node should be recognized after projection with WITH + MERGE node
    Given an empty graph
    When executing query:
      """
      CREATE (a)
      WITH a
      MERGE ()
      CREATE (b)
      CREATE (a)<-[:T]-(b)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |

  Scenario: [12] WITH-MERGE-CREATE: A bound node should be recognized after projection with WITH + MERGE pattern
    Given an empty graph
    When executing query:
      """
      CREATE (a)
      WITH a
      MERGE (x)
      MERGE (y)
      MERGE (x)-[:T]->(y)
      CREATE (b)
      CREATE (a)<-[:T]-(b)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 2 |

  Scenario: [13] Merge followed by multiple creates
    Given an empty graph
    When executing query:
      """
      MERGE (t:T {id: 42})
      CREATE (f:R)
      CREATE (t)-[:REL]->(f)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |
      | +labels        | 2 |
      | +properties    | 1 |
