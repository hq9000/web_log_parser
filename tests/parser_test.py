import os
import shutil
import sqlite3
from web_log_parser.parser import Parser


def test_something(tmpdir):
    script_dir = os.path.dirname(__file__)
    log_path = os.path.join(script_dir, "fixture/example.log")

    tmp_log_path = tmpdir.join("access.log")
    shutil.copyfile(log_path, tmp_log_path)

    parser = Parser(
        log_path=tmp_log_path,
        db_path=tmpdir.join("test.db"),
        last_cursor_position_file_path=tmpdir.join("last_cursor_position.txt"),
    )

    parser.parse()

    assert 3 == _get_number_of_rows(parser.sqlite_db_path)
    parser.parse()
    assert 3 == _get_number_of_rows(parser.sqlite_db_path)
    os.remove(tmp_log_path)
    parser.parse()
    assert 3 == _get_number_of_rows(parser.sqlite_db_path)
    shutil.copyfile(log_path, tmp_log_path)
    parser.parse()
    assert 6 == _get_number_of_rows(parser.sqlite_db_path)

    # add the contents of tmp_log_path to the file itself
    with open(tmp_log_path, "r") as f:
        lines = f.readlines()
    with open(tmp_log_path, "a") as f:
        f.writelines(lines)  # add the contents again

    parser.parse()
    assert 9 == _get_number_of_rows(parser.sqlite_db_path)

    shutil.copyfile(log_path, tmp_log_path)
    parser.parse()
    assert 12 == _get_number_of_rows(parser.sqlite_db_path)

    last_row = _get_last_row(parser.sqlite_db_path)
    assert last_row[0] == 12


def _get_number_of_rows(sqlite_db_path: str) -> int:
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM logs")
    count = cursor.fetchone()[0]
    conn.close()
    return count


def _get_last_row(sqlite_db_path: str):
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM logs ORDER BY id DESC LIMIT 1")
    row = cursor.fetchone()
    conn.close()
    return row
