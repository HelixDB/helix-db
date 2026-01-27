QUERY TestQuery() =>
    test1 <- N<Test>
    result <- test1::ORDER<Asc>(_::Out<TestEdge>::COUNT)::FIRST
    RETURN result


