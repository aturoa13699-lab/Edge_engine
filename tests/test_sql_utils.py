from app.sql_utils import split_sql_statements


def test_splitter_respects_quotes_and_semicolons():
    sql = "CREATE TABLE t(a text); INSERT INTO t VALUES('x;y'); SELECT 1;"
    stmts = split_sql_statements(sql)
    assert len(stmts) == 3
    assert "x;y" in stmts[1]
