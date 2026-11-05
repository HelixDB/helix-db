#!/usr/bin/env python3
"""Check POST /v2/sql against a running image.

`seed` writes a small movie graph with SQL and checks reads, joins, grouping,
writes and errors; `verify` checks, after a restart, that the SQL writes
persisted and read the same through SQL and Cypher.
"""
import json
import sys
import urllib.error
import urllib.request


def post(port, path, body, headers=None):
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", **(headers or {})},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read())


def sql(port, query, parameters=None, mappings=None, headers=None):
    body = {"query": query}
    if parameters is not None:
        body["parameters"] = parameters
    if mappings is not None:
        body["mappings"] = mappings
    status, value = post(port, "/v2/sql", body, headers)
    assert status == 200, (query, status, value)
    return value


def rows(port, query, parameters=None, mappings=None):
    return sql(port, query, parameters, mappings)["rows"]


def error(port, query, headers=None):
    status, value = post(port, "/v2/sql", {"query": query}, headers)
    assert status != 200, (query, value)
    return status, value


DURABLE = {"X-Helix-Await-Durable": "true"}
MOVIES = {"tables": {"people": "SqlPerson", "movies": "SqlMovie"}}


def seed(port):
    sql(port, "INSERT INTO people (name, born, dept) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)",
        ["Ada", 1815, "math", "Grace", 1906, "navy", "Alan", 1912, "math"],
        MOVIES, DURABLE)
    sql(port, "INSERT INTO movies (title, released) VALUES ('Hidden', 2016), ('Enigma', 2014)",
        mappings=MOVIES, headers=DURABLE)
    sql(port, "INSERT INTO SqlPerson_ACTED_IN_SqlMovie (SqlPerson.name, SqlMovie.title, role) "
              "VALUES ('Ada', 'Hidden', 'lead'), ('Alan', 'Enigma', 'self'), ('Grace', 'Hidden', 'cameo')",
        headers=DURABLE)

    assert sql(port, "SELECT name, born FROM people p WHERE born > ? ORDER BY born DESC",
               [1900], MOVIES) == {"columns": ["name", "born"],
                                   "rows": [["Alan", 1912], ["Grace", 1906]]}
    joined = [["Ada", "Hidden", "lead"], ["Alan", "Enigma", "self"], ["Grace", "Hidden", "cameo"]]
    assert rows(port, "SELECT p.name, m.title, r.role FROM people p "
                      "JOIN ACTED_IN r ON r.person = p.id JOIN movies m ON m.id = r.movie "
                      "ORDER BY p.name", mappings=MOVIES) == joined
    assert rows(port, "SELECT sqlperson_name, sqlmovie_title, role "
                      "FROM SqlPerson_ACTED_IN_SqlMovie ORDER BY sqlperson_name") == joined
    assert rows(port, "SELECT m.title, count(*) AS actors FROM people p NATURAL JOIN ACTED_IN r "
                      "NATURAL JOIN movies m GROUP BY m.title ORDER BY actors DESC",
                mappings=MOVIES) == [["Hidden", 2], ["Enigma", 1]]
    assert rows(port, "SELECT dept, count(*) FROM people p GROUP BY dept HAVING count(*) > 1",
                mappings=MOVIES) == [["math", 2]]
    assert rows(port, "SELECT upper(dept), min(born) FROM people p GROUP BY dept ORDER BY 1",
                mappings=MOVIES) == [["MATH", 1815], ["NAVY", 1906]]

    sql(port, "UPDATE people SET born = born + 1 WHERE name = :who", {"who": "Ada"}, MOVIES, DURABLE)
    status, value = error(port, "DELETE FROM SqlPerson WHERE name = 'Ada'")
    assert (status, value["error"], value["details"]["detail"]) == (
        400, "constraint_verification_failed", "delete_connected_node"), value
    status, value = error(port, "SELECT 1 UNION SELECT 2")
    assert (status, value["details"]["detail"], value["details"]["phase"]) == (
        400, "union", "compile"), value
    status, value = error(port, "INSERT INTO SqlPerson (name) VALUES ('warm')",
                          {"X-Helix-Warm": "true"})
    assert (status, value["error"]) == (400, "invalid_request_option"), value
    status, value = error(port, "SELECT sqrt(p.born) FROM SqlPerson p")
    assert value["details"]["span"] == {"start": 7, "end": 18}, value


def verify(port):
    assert rows(port, "SELECT name, born FROM people p ORDER BY name", mappings=MOVIES) == [
        ["Ada", 1816], ["Alan", 1912], ["Grace", 1906]]
    status, cypher = post(port, "/v2/cypher", {"query":
        "MATCH (p:SqlPerson)-[r:ACTED_IN]->(m:SqlMovie) "
        "RETURN p.name AS name, m.title AS title ORDER BY name"})
    assert status == 200, cypher
    assert rows(port, "SELECT p.name AS name, m.title AS title FROM people p "
                      "NATURAL JOIN ACTED_IN r NATURAL JOIN movies m ORDER BY name",
                mappings=MOVIES) == cypher["rows"]
    sql(port, "TRUNCATE TABLE movies CASCADE", mappings=MOVIES, headers=DURABLE)
    assert rows(port, "SELECT count(*) FROM SqlPerson_ACTED_IN_SqlMovie") == [[0]]
    assert rows(port, "SELECT count(*) FROM people p", mappings=MOVIES) == [[3]]


if __name__ == "__main__":
    {"seed": seed, "verify": verify}[sys.argv[1]](int(sys.argv[2]))
    print(f"SQL {sys.argv[1]} contracts passed")
