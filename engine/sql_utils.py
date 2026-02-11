"""SQL utilities.

We avoid fragile `sql.split(';')` behavior by implementing a small splitter that
respects:
  - single/double quotes
  - line and block comments
  - PostgreSQL dollar-quoted blocks ($$...$$ or $tag$...$tag$)

This is *not* a full SQL parser, but it's robust enough for typical schema
files containing tables, views, indexes, and simple functions.
"""

from __future__ import annotations

from typing import List


def split_sql_statements(sql: str) -> List[str]:
    stmts: List[str] = []
    buf: List[str] = []

    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False
    dollar_tag: str | None = None

    i = 0
    n = len(sql)

    def flush():
        s = "".join(buf).strip()
        buf.clear()
        if s:
            stmts.append(s)

    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""

        # Inside line comment
        if in_line_comment:
            buf.append(ch)
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue

        # Inside block comment
        if in_block_comment:
            buf.append(ch)
            if ch == "*" and nxt == "/":
                buf.append(nxt)
                i += 2
                in_block_comment = False
            else:
                i += 1
            continue

        # Inside dollar-quoted block
        if dollar_tag is not None:
            if sql.startswith(dollar_tag, i):
                buf.append(dollar_tag)
                i += len(dollar_tag)
                dollar_tag = None
            else:
                buf.append(ch)
                i += 1
            continue

        # Start comments
        if not in_single and not in_double:
            if ch == "-" and nxt == "-":
                buf.append(ch)
                buf.append(nxt)
                i += 2
                in_line_comment = True
                continue
            if ch == "/" and nxt == "*":
                buf.append(ch)
                buf.append(nxt)
                i += 2
                in_block_comment = True
                continue

        # Start dollar quote ($$ or $tag$)
        if not in_single and not in_double and ch == "$":
            j = i + 1
            while j < n and sql[j] != "$" and sql[j] not in "\r\n\t ":
                j += 1
            if j < n and sql[j] == "$":
                tag = sql[i : j + 1]  # includes closing $
                dollar_tag = tag
                buf.append(tag)
                i = j + 1
                continue

        # Toggle quotes (respect escaped quotes)
        if ch == "'" and not in_double:
            if in_single and nxt == "'":
                # escaped single quote
                buf.append(ch)
                buf.append(nxt)
                i += 2
                continue
            in_single = not in_single
            buf.append(ch)
            i += 1
            continue

        if ch == '"' and not in_single:
            if in_double and nxt == '"':
                buf.append(ch)
                buf.append(nxt)
                i += 2
                continue
            in_double = not in_double
            buf.append(ch)
            i += 1
            continue

        # Split on semicolon only when not inside any special region
        if ch == ";" and not in_single and not in_double:
            flush()
            i += 1
            continue

        buf.append(ch)
        i += 1

    flush()
    return stmts
