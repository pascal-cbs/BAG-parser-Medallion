import os
import shutil
import sqlite3
from argparse import ArgumentParser

import config
import utils

parser = ArgumentParser(description='Convert the tables of the SQLite database to Apache Iceberg tables using DuckDB')

helpText = f"SQLite database to convert (default: {config.file_db_sqlite})"
parser.add_argument('-i', '--input', default=config.file_db_sqlite, help=helpText)

helpText = f"Output folder. Every table becomes a subfolder (default: {config.dir_iceberg})"
parser.add_argument('-o', '--output', default=config.dir_iceberg, help=helpText)

helpText = "Only convert these tables (default: all tables)"
parser.add_argument('-t', '--tables', nargs='+', help=helpText)

args = parser.parse_args()

try:
    import duckdb
except ImportError:
    utils.print_log_error("The duckdb package is required for this script. Install it with: uv add duckdb")
    raise SystemExit(1)

if not os.path.isfile(args.input):
    utils.print_log_error(f"SQLite database not found: {args.input}")
    raise SystemExit(1)

utils.print_log(f"DuckDB version {duckdb.__version__}")

con = duckdb.connect()
# The extensions are downloaded the first time they are installed.
con.execute("INSTALL sqlite")
con.execute("INSTALL iceberg")
con.execute("LOAD sqlite")
con.execute("LOAD iceberg")
# SQLite does not enforce column types: numeric columns can contain empty strings. Read everything as text and cast
# using the declared SQLite column types, so empty values become NULL.
con.execute("SET sqlite_all_varchar = true")
con.execute(f"ATTACH '{args.input}' AS bag (TYPE sqlite, READ_ONLY)")

tables_in_db = [row[0] for row in con.execute(
    "SELECT table_name FROM duckdb_tables() WHERE database_name = 'bag' ORDER BY table_name").fetchall()]

tables = args.tables or tables_in_db
unknown_tables = [table for table in tables if table not in tables_in_db]
if unknown_tables:
    utils.print_log_error(f"Tables not found in {args.input}: {', '.join(unknown_tables)}")
    raise SystemExit(1)

duckdb_types = {'INTEGER': 'BIGINT', 'FLOAT': 'DOUBLE'}
db_sqlite = sqlite3.connect(f'file:{args.input}?mode=ro', uri=True)


def select_columns(table):
    columns = []
    for _, name, declared_type, *_ in db_sqlite.execute(f'PRAGMA table_info("{table}")'):
        duckdb_type = duckdb_types.get(declared_type.upper())
        columns.append(f'TRY_CAST(NULLIF("{name}", \'\') AS {duckdb_type}) AS "{name}"' if duckdb_type else f'"{name}"')
    return ', '.join(columns)


os.makedirs(args.output, exist_ok=True)

for table in tables:
    folder = os.path.join(args.output, table)
    count = con.execute(f'SELECT COUNT(*) FROM bag."{table}"').fetchone()[0]
    if count == 0:
        utils.print_log(f"Skip {table}: no records")
        continue

    utils.print_log(f"Convert {table}: {count:n} records")
    # An Iceberg table folder can't be overwritten, so start with an empty folder
    shutil.rmtree(folder, ignore_errors=True)
    con.execute(f'COPY (SELECT {select_columns(table)} FROM bag."{table}") TO \'{folder}\' (FORMAT iceberg)')

utils.print_log(f"Ready: Iceberg tables saved in {args.output}")
