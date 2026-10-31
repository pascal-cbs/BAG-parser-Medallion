import os
import shutil
import sqlite3
from argparse import ArgumentParser

import config
import utils

parser = ArgumentParser(description='Convert the tables of the SQLite database to Apache Iceberg tables using DuckDB, '
                                    'organized in medallion layers (silver and gold)')

helpText = f"SQLite database to convert (default: {config.file_db_sqlite})"
parser.add_argument('-i', '--input', default=config.file_db_sqlite, help=helpText)

helpText = f"Output folder. Tables are saved in layer/table subfolders (default: {config.dir_iceberg})"
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

layers = config.medallion_layers
default_layer = config.medallion_default_layer

# Gold aggregates, calculated from the Iceberg tables of the layers below.
# SQLite's SUBSTR(postcode, 0, 5) is the same as LEFT(postcode, 4). MIN(woonplaats) makes the result deterministic.
gold_postcode_stats = {
    'postcode4_stats': 'LEFT(a.postcode, 4)',
    'postcode5_stats': 'LEFT(a.postcode, 5)',
    'postcode6_stats': 'a.postcode',
}

duckdb_types = {'INTEGER': 'BIGINT', 'FLOAT': 'DOUBLE'}
db_sqlite = sqlite3.connect(f'file:{args.input}?mode=ro', uri=True)


def select_columns(table):
    columns = []
    for _, name, declared_type, *_ in db_sqlite.execute(f'PRAGMA table_info("{table}")'):
        duckdb_type = duckdb_types.get(declared_type.upper())
        columns.append(f'TRY_CAST(NULLIF("{name}", \'\') AS {duckdb_type}) AS "{name}"' if duckdb_type else f'"{name}"')
    return ', '.join(columns)


os.makedirs(args.output, exist_ok=True)


def table_folder(table):
    return os.path.join(args.output, layers.get(table, default_layer), table)


def write_iceberg(select_sql, folder):
    # An Iceberg table folder can't be overwritten, so start with an empty folder
    shutil.rmtree(folder, ignore_errors=True)
    os.makedirs(os.path.dirname(folder), exist_ok=True)
    con.execute(f"COPY ({select_sql}) TO '{folder}' (FORMAT iceberg)")


for table in tables:
    folder = table_folder(table)
    count = con.execute(f'SELECT COUNT(*) FROM bag."{table}"').fetchone()[0]
    if count == 0:
        utils.print_log(f"Skip {table}: no records")
        continue

    utils.print_log(f"Convert {table}: {count:n} records")
    write_iceberg(f'SELECT {select_columns(table)} FROM bag."{table}"', folder)

if not args.tables or 'adressen' in args.tables:
    folder_adressen = table_folder('adressen')
    folder_woonplaatsen = table_folder('woonplaatsen')
    if os.path.isdir(folder_adressen) and os.path.isdir(folder_woonplaatsen):
        for name, postcode_expression in gold_postcode_stats.items():
            utils.print_log(f"Create gold table {name}")
            write_iceberg(f"""
                SELECT
                  {postcode_expression}  AS {name.split('_')[0]},
                  AVG(a.latitude)        AS center_lat,
                  AVG(a.longitude)       AS center_lon,
                  COUNT(1)               AS aantal_adressen,
                  MIN(w.naam)            AS woonplaats
                FROM iceberg_scan('{folder_adressen}') a
                  LEFT JOIN iceberg_scan('{folder_woonplaatsen}') w ON a.woonplaats_id = w.id
                WHERE a.postcode <> ''
                GROUP BY 1""", os.path.join(args.output, 'gold', name))
    else:
        utils.print_log_warning("Postcode statistics skipped: the adressen and woonplaatsen tables are needed")

utils.print_log(f"Ready: Iceberg tables saved in {args.output}")
