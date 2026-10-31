import importlib.util
import locale
import os

version = 116
version_date = '31 October 2026'

# At startup the parser checks if a newer version is available on GitHub.
check_for_updates = True

# Change this url if you use a fork.
update_check_url = 'https://raw.githubusercontent.com/digitaldutch/BAG_parser/master/config.py'

# Coordinate system of the latitude and longitude fields (and the geometry):
# 'ETRS89' (default): The European standard that the Rijksdriehoekscoordinaten are tied to. These coordinates never change.
#                     They can be used as WGS84, see the Coordinates section in README.md.
# 'WGS84':            Exact WGS84 coordinates for the epoch below. ETRS89 and WGS84 are about 0.9 meter apart in the
#                     Netherlands and the difference grows about 2.4 cm per year, so WGS84 coordinates depend on the epoch.
coordinate_system = 'ETRS89'

# Epoch (date) for the WGS84 coordinates, if coordinate_system is 'WGS84'. Use a decimal year (2026.5) or a date ('2026-06-30').
# None means today (the date of the import). The epoch is shown in the log and saved in the bag_info table.
# Use a fixed epoch if you want the same coordinates every time. Do not mix databases with different epochs if
# centimeters matter.
wgs84_epoch = None

# Dutch number formatting (e.g. 1.234.567) in log messages. Falls back to the system default if the locale is not installed.
try:
    locale.setlocale(locale.LC_ALL, 'nl_NL')
except locale.Error:
    locale.setlocale(locale.LC_ALL, '')

# Location of the BAG zip file downloaded from kadaster. See README.md
file_bag = 'input/lvbag-extract-nl.zip'

# Location of the gemeenten file downloaded from cbs.nl. See README.md
file_gemeenten = 'input/gemeenten.csv'

# Output SQLite database with parsed BAG
file_db_sqlite = 'output/bag.sqlite'

# Output folder of convert_to_iceberg.py. The Iceberg tables are saved in medallion layer subfolders: silver, gold and metadata.
dir_iceberg = 'output/iceberg'

# Medallion layer per table, used by convert_to_iceberg.py and convert_to_trino.py. The raw BAG XML (input/bag.zip) is the
# bronze layer. The parsed, cleaned and typed tables are silver. 'adressen' is the denormalized data product (gold).
# bag_info is lineage metadata. Unlisted tables go to medallion_default_layer.
medallion_layers = {
    'bag_info': 'metadata',
    'adressen': 'gold',
}
medallion_default_layer = 'silver'

# Trino server used by convert_to_trino.py. The tables are created in the catalog below, in a schema per medallion layer
# (silver, gold and metadata). Use an Iceberg catalog. A password can be set in the TRINO_PASSWORD environment variable
# (needs https).
trino_host = 'localhost'
trino_port = 8080
trino_http_scheme = 'http'
trino_user = 'bag'
trino_catalog = 'iceberg'

# How convert_to_trino.py sends the records:
# 's3' (fast):  DuckDB writes the tables as Parquet files directly to the S3 storage of the Iceberg catalog, in the folder of
#               each table. Trino adds these files to the table (add_files procedure, needs
#               iceberg.add-files-procedure.enabled=true in the catalog properties). Needs access to that storage, see below.
# 'insert':     INSERT statements with the records. Works with any catalog and without access to the storage, but it is slow:
#               Trino has to parse every record (about 5,000 records per second).
trino_method = 's3'

# S3 storage of the Iceberg catalog, as seen from this computer (method 's3'). The defaults are for trino/docker-compose.yml.
# The keys can also be set in the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.
trino_s3_endpoint = 'localhost:8333'
trino_s3_use_ssl = False
trino_s3_url_style = 'path'
trino_s3_region = 'us-east-1'
trino_s3_access_key = 'admin'
trino_s3_secret_key = 'password'

# Method 'insert': maximum length of a Trino query (query.max-length on the server, default 1,000,000 characters).
# Records are sent in INSERT statements of at most this length.
trino_max_query_length = 1_000_000

# Method 'insert': number of INSERT statements that convert_to_trino.py sends at the same time.
trino_workers = 4

# Log-file containing progress, warnings and error messages. This info is also written to the console.
file_log = 'output/bag_importer.log'

# The parser creates an 'adressen' table merging the data of nummers, panden, verblijfsobjecten, ligplaatsen and
# standplaatsen tables into one single table. It only contains active addresses.
create_adressen_table = True

# Only add active records. Historical data of no longer active records are removed.
# The 'adressen' table can only be created if set to True.
active_only = True

# If an adressen table is created some BAG tables are no longer needed and can be deleted:
# nummers, panden, verblijfsobjecten, ligplaatsen and standplaatsen. Set to False if you want to keep these tables.
# You can also delete these tables afterward using the utils_sqlite_shrink.py script.
delete_no_longer_needed_bag_tables = False

# Public spaces with names longer than 24 characters also have a shortened name. Set to true to make short names the
# default if available.
# https://imbag.github.io/praktijkhandleiding/artikelen/hoe-wordt-de-verkorte-schrijfwijze-van-een-openbare-ruimte-bepaald
use_short_street_names = False

# Enable if you want to parse geometry data for woonplaatsen, panden, ligplaatsen and standplaatsen.
# The data is stored in polygon geojson format in the geometry field.
# And the database size will increase from 1.7GB to 16GB. Or 7GB with delete_no_longer_needed_bag_tables enabled.
# Parsing will also take a few minutes more.
parse_geometries = False

# The BAG sometimes contains addresses without a valid public space id. Generally those are invalid addresses.
# They will be automatically deleted if the total number of invalid addresses is lower than the number below.
# Set to 0 if you prefer warning messages and manually check and correct these entries yourself.
delete_addresses_without_public_spaces_if_less_than = 100

# The parser uses multiprocessing to speed up parsing the data. The number of physical CPU cores is a good choice.
# Using logical cores hardly makes a difference. The psutil module automatically determines the physical CPU core count.
# If it is not installed, the number of logical CPU cores is used (os.cpu_count).
# You can also set cpu_cores_used yourself below, after these lines.

has_psutil = importlib.util.find_spec("psutil") is not None
if has_psutil:
    import psutil
    cpu_cores_used = psutil.cpu_count(False)
else:
    # os.cpu_count() returns None if the number can't be determined
    cpu_cores_used = os.cpu_count() or 8

# Windows can't start more than 61 processes in a process pool
if os.name == 'nt':
    cpu_cores_used = min(cpu_cores_used, 61)


