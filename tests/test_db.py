from sqlalchemy.engine import URL


def test_get_engine_uses_alias_when_database_url_missing(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DATABASE_PRIVATE_URL", "postgresql://u:p@localhost:5432/dbx")

    captured: dict[str, URL] = {}

    def fake_create_engine(url, **kwargs):
        captured["url"] = url
        return object()

    import engine.db as db

    monkeypatch.setattr(db, "create_engine", fake_create_engine)

    db.get_engine()

    assert str(captured["url"]).startswith(
        "postgresql+psycopg://u:***@localhost:5432/dbx"
    )


def test_get_engine_adds_sslmode_require_when_flag_set(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/dbx")
    monkeypatch.setenv("REQUIRE_DB_SSL", "1")
    monkeypatch.delenv("DB_SSLMODE", raising=False)

    captured: dict[str, URL] = {}

    def fake_create_engine(url, **kwargs):
        captured["url"] = url
        return object()

    import engine.db as db

    monkeypatch.setattr(db, "create_engine", fake_create_engine)

    db.get_engine()

    assert captured["url"].query.get("sslmode") == "require"
