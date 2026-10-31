import math
import os
import re
import sqlite3
import threading
import uuid
from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor, FIRST_COMPLETED, wait

import config
import utils
from statusbar import StatusUpdater

parser = ArgumentParser(description='Send the tables of the SQLite database to Trino, organized in medallion layers '
                                    '(a schema for silver, gold and metadata)')

helpText = f"SQLite database to send (default: {config.file_db_sqlite})"
parser.add_argument('-i', '--input', default=config.file_db_sqlite, help=helpText)

helpText = "Only send these tables (default: all tables)"
parser.add_argument('-t', '--tables', nargs='+', help=helpText)

helpText = f"Trino host (default: {config.trino_host})"
parser.add_argument('--host', default=config.trino_host, help=helpText)

helpText = f"Trino port (default: {config.trino_port})"
parser.add_argument('--port', type=int, default=config.trino_port, help=helpText)

helpText = f"Trino user (default: {config.trino_user})"
parser.add_argument('--user', default=config.trino_user, help=helpText)

helpText = f"Trino catalog. The schemas silver, gold and metadata are created in it (default: {config.trino_catalog})"
parser.add_argument('-c', '--catalog', default=config.trino_catalog, help=helpText)

helpText = f"http or https (default: {config.trino_http_scheme})"
parser.add_argument('--http-scheme', default=config.trino_http_scheme, choices=['http', 'https'], help=helpText)

helpText = (f"s3: write Parquet files to the S3 storage of the Iceberg catalog with DuckDB (fast). insert: send the records "
            f"in INSERT statements (slow). See config.py (default: {config.trino_method})")
parser.add_argument('-m', '--method', default=config.trino_method, choices=['s3', 'insert'], help=helpText)

helpText = f"Method insert: number of INSERT statements sent at the same time (default: {config.trino_workers})"
parser.add_argument('-w', '--workers', type=int, default=config.trino_workers, help=helpText)

args = parser.parse_args()

try:
    import trino
except ImportError:
    utils.print_log_error("The trino package is required for this script. Install it with: uv add trino")
    raise SystemExit(1)

if args.method == 's3':
    try:
        import duckdb
    except ImportError:
        utils.print_log_error("The duckdb package is required for method s3. Install it with: uv add duckdb")
        raise SystemExit(1)

if not os.path.isfile(args.input):
    utils.print_log_error(f"SQLite database not found: {args.input}")
    raise SystemExit(1)

layers = config.medallion_layers
default_layer = config.medallion_default_layer

# Gold aggregates, calculated in Trino from the tables of the layers below. Same as in convert_to_iceberg.py.
gold_postcode_stats = {
    'postcode4_stats': 'substr(a.postcode, 1, 4)',
    'postcode5_stats': 'substr(a.postcode, 1, 5)',
    'postcode6_stats': 'a.postcode',
}

trino_types = {'INTEGER': 'BIGINT', 'FLOAT': 'DOUBLE'}

# Keep a margin for the 'INSERT INTO ... VALUES' part
max_statement_length = int(config.trino_max_query_length * 0.95)

password = os.environ.get('TRINO_PASSWORD')
auth = trino.auth.BasicAuthentication(args.user, password) if password else None
connection_info = threading.local()


def trino_cursor():
    # trino connections are not thread safe. Each worker thread gets its own connection.
    if not hasattr(connection_info, 'connection'):
        connection_info.connection = trino.dbapi.connect(host=args.host, port=args.port, user=args.user,
                                                         catalog=args.catalog, http_scheme=args.http_scheme, auth=auth,
                                                         source='BAG-parser')
    return connection_info.connection.cursor()


def trino_execute(sql):
    cursor = trino_cursor()
    cursor.execute(sql)
    # The statement only finishes if all results are fetched
    return cursor.fetchall()


def quote(name):
    return '"' + name.replace('"', '""') + '"'


def table_name(table, layer=None):
    return f'{quote(args.catalog)}.{quote(layer or layers.get(table, default_layer))}.{quote(table)}'


def literal_bigint(value):
    # SQLite does not enforce column types: numeric columns can contain empty strings or text. Those become NULL.
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return 'NULL'


def literal_double(value):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return 'NULL'
    if not math.isfinite(value):
        return 'NULL'
    # A number with an exponent is a DOUBLE literal in Trino. Without it, it would be a DECIMAL.
    text = repr(value).upper()
    return text if 'E' in text else text + 'E0'


def literal_varchar(value):
    return "'" + str(value).replace("'", "''") + "'"


literal_functions = {'BIGINT': literal_bigint, 'DOUBLE': literal_double, 'VARCHAR': literal_varchar}


def create_table(table, columns):
    name = table_name(table)
    trino_execute(f'DROP TABLE IF EXISTS {name}')
    trino_execute(f"CREATE TABLE {name} ({', '.join(f'{quote(column)} {data_type}' for column, data_type in columns)})")
    return name


def send_table_s3(table, columns, count):
    utils.print_log(f"Send {table}: {count:n} records (Parquet files)")
    name = create_table(table, columns)

    # Write the files in the folder of the table, so DROP TABLE also deletes them
    location = re.search(r"location = '([^']+)'", trino_execute(f'SHOW CREATE TABLE {name}')[0][0]).group(1)
    folder = f"{location.rstrip('/')}/data/bag-{uuid.uuid4().hex}/"

    # SQLite does not enforce column types: numeric columns can contain empty strings or text. Those become NULL.
    select_columns = ', '.join(f'"{column}"' if data_type == 'VARCHAR' else
                               f'TRY_CAST(NULLIF("{column}", \'\') AS {data_type}) AS "{column}"'
                               for column, data_type in columns)
    # DuckDB only knows the s3:// scheme
    duckdb_folder = re.sub(r'^s3[an]://', 's3://', folder)
    con.execute(f"COPY (SELECT {select_columns} FROM bag.{quote(table)}) TO '{duckdb_folder}' "
                f"(FORMAT parquet, FILE_SIZE_BYTES '256MB')")

    added_files = trino_execute(f"ALTER TABLE {name} EXECUTE add_files(location => {literal_varchar(folder)}, "
                                f"format => 'PARQUET')")
    utils.print_log(f"Added {added_files[0][1] if added_files else '?'} Parquet file(s) to {name}")


def send_table_insert(table, columns, count):
    utils.print_log(f"Send {table}: {count:n} records (INSERT statements)")
    name = create_table(table, columns)

    literals = [literal_functions[data_type] for _, data_type in columns]
    insert_sql = f'INSERT INTO {name} VALUES\n'
    column_names = ', '.join(quote(column) for column, _ in columns)

    status = StatusUpdater()
    status.start(count, table)
    sent_count = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        running = {}

        def submit(rows):
            nonlocal sent_count
            # Limit the number of waiting statements, so the SQLite table is not read into memory at once
            while len(running) >= args.workers * 2:
                done, _ = wait(running, return_when=FIRST_COMPLETED)
                for future in done:
                    future.result()
                    sent_count += running.pop(future)
                    status.update(sent_count)
            future = executor.submit(trino_execute, insert_sql + ',\n'.join(rows))
            running[future] = len(rows)

        rows = []
        length = len(insert_sql)
        db_sqlite = sqlite3.connect(f'file:{args.input}?mode=ro', uri=True)
        for record in db_sqlite.execute(f'SELECT {column_names} FROM {quote(table)}'):
            row = '(' + ', '.join('NULL' if value is None else literal(value)
                                  for literal, value in zip(literals, record)) + ')'
            if rows and length + len(row) + 2 > max_statement_length:
                submit(rows)
                rows = []
                length = len(insert_sql)
            rows.append(row)
            length += len(row) + 2
        db_sqlite.close()
        if rows:
            submit(rows)

        for future in running:
            future.result()
            sent_count += running[future]
    status.update(sent_count)
    status.end()

    if is_iceberg:
        # Every INSERT adds a data file. Merge them into larger files.
        trino_execute(f'ALTER TABLE {name} EXECUTE optimize')


try:
    trino_execute('SELECT 1')
except Exception as e:
    utils.print_log_error(f"Can't connect to Trino at {args.http_scheme}://{args.host}:{args.port}: {e}")
    raise SystemExit(1)

connectors = trino_execute(f"SELECT connector_name FROM system.metadata.catalogs "
                           f"WHERE catalog_name = {literal_varchar(args.catalog)}")
if not connectors:
    utils.print_log_error(f"Catalog not found on the Trino server: {args.catalog}")
    raise SystemExit(1)
is_iceberg = connectors[0][0] == 'iceberg'
if not is_iceberg:
    if args.method == 's3':
        utils.print_log_error(f"Catalog {args.catalog} uses the {connectors[0][0]} connector. Method s3 needs an iceberg "
                              f"catalog. Use --method insert")
        raise SystemExit(1)
    utils.print_log_warning(f"Catalog {args.catalog} uses the {connectors[0][0]} connector. An iceberg catalog is recommended")

utils.print_log(f"Trino {trino_execute('SELECT version()')[0][0]} at {args.host}:{args.port}, catalog {args.catalog}")

if args.method == 's3':
    con = duckdb.connect()
    # The extensions are downloaded the first time they are installed.
    con.execute("INSTALL sqlite")
    con.execute("INSTALL httpfs")
    con.execute("LOAD sqlite")
    con.execute("LOAD httpfs")
    # Read everything as text. send_table_s3 casts the numeric columns, so empty values become NULL.
    con.execute("SET sqlite_all_varchar = true")
    con.execute(f"ATTACH {literal_varchar(args.input)} AS bag (TYPE sqlite, READ_ONLY)")
    s3_key_id = os.environ.get('AWS_ACCESS_KEY_ID', config.trino_s3_access_key)
    s3_secret = os.environ.get('AWS_SECRET_ACCESS_KEY', config.trino_s3_secret_key)
    con.execute(f"""CREATE SECRET (TYPE s3, KEY_ID {literal_varchar(s3_key_id)}, SECRET {literal_varchar(s3_secret)},
                    REGION {literal_varchar(config.trino_s3_region)}, ENDPOINT {literal_varchar(config.trino_s3_endpoint)},
                    URL_STYLE {literal_varchar(config.trino_s3_url_style)}, USE_SSL {str(config.trino_s3_use_ssl).lower()})""")
    utils.print_log(f"DuckDB version {duckdb.__version__}, S3 storage at {config.trino_s3_endpoint}")

db_sqlite = sqlite3.connect(f'file:{args.input}?mode=ro', uri=True)
tables_in_db = [row[0] for row in db_sqlite.execute(
    "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]

tables = args.tables or tables_in_db
unknown_tables = [table for table in tables if table not in tables_in_db]
if unknown_tables:
    utils.print_log_error(f"Tables not found in {args.input}: {', '.join(unknown_tables)}")
    raise SystemExit(1)

for layer in sorted({layers.get(table, default_layer) for table in tables}):
    trino_execute(f'CREATE SCHEMA IF NOT EXISTS {quote(args.catalog)}.{quote(layer)}')

for table in tables:
    count = db_sqlite.execute(f'SELECT COUNT(*) FROM {quote(table)}').fetchone()[0]
    if count == 0:
        utils.print_log(f"Skip {table}: no records")
        continue

    columns = [(name, trino_types.get(declared_type.upper(), 'VARCHAR'))
               for _, name, declared_type, *_ in db_sqlite.execute(f'PRAGMA table_info({quote(table)})')]
    if args.method == 's3':
        send_table_s3(table, columns, count)
    else:
        send_table_insert(table, columns, count)

db_sqlite.close()


def trino_table_exists(table):
    layer = layers.get(table, default_layer)
    return bool(trino_execute(f"SELECT 1 FROM {quote(args.catalog)}.information_schema.tables "
                              f"WHERE table_schema = {literal_varchar(layer)} AND table_name = {literal_varchar(table)}"))


if not args.tables or 'adressen' in args.tables:
    if trino_table_exists('adressen') and trino_table_exists('woonplaatsen'):
        for name, postcode_expression in gold_postcode_stats.items():
            utils.print_log(f"Create gold table {name}")
            trino_execute(f'DROP TABLE IF EXISTS {table_name(name, "gold")}')
            trino_execute(f"""
                CREATE TABLE {table_name(name, 'gold')} AS
                SELECT
                  {postcode_expression}  AS {name.split('_')[0]},
                  AVG(a.latitude)        AS center_lat,
                  AVG(a.longitude)       AS center_lon,
                  COUNT(1)               AS aantal_adressen,
                  MIN(w.naam)            AS woonplaats
                FROM {table_name('adressen')} a
                  LEFT JOIN {table_name('woonplaatsen')} w ON a.woonplaats_id = w.id
                WHERE a.postcode <> ''
                GROUP BY 1""")
    else:
        utils.print_log_warning("Postcode statistics skipped: the adressen and woonplaatsen tables are needed")

utils.print_log(f"Ready: tables sent to Trino catalog {args.catalog}")
