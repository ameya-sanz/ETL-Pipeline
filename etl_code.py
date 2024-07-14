import pandas as pd
import pyodbc
import os
import re
import urllib
from sqlalchemy import create_engine, text



mssql_conn = urllib.parse.quote_plus(
 'Data Source Name=MSSQLDataSource;'
 'Driver={ODBC Driver 17 for SQL Server};'
 'Server=SANZ\\SQLEXPRESS;'
 'DATABASE=AdventureWorksDW2019;'
 'Trusted_connection=yes;'
)


# PostgreSQL connection details
uid = os.environ.get("PGUID")
pwd = os.environ.get("PGPASS")
server = 'localhost'  # Replace with your PostgreSQL server address if different
database = 'AdventureWorksDW2019'



### Create engines

mssqlserver_engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(mssql_conn))

# Construct the connection string
#postgres_conn = f'postgresql+psycopg2://{uid}:{pwd}@{server}/{database}'
# Create the engine
#postgres_engine = create_engine(postgres_conn)
#postgres_uri = f"postgres+psycopg2://{os.environ.get('postgres_user')}:{os.environ.get('postgres_pass')}@localhost:5432/{database_name}"
#postgres_engine = create_engine(postgres_uri)

postgres_engine = create_engine('postgresql+psycopg2://postgres:RS1234_Career@localhost/AdventureWorksDW2019')


################################################################
print(f"Engines created for database {database}")
print()
print()
################################################################


### Query all tables, including views

mssqlserver_table_query = text('''

    SELECT
          t.name AS table_name
        , s.name AS schema_name
    FROM sys.tables t
    INNER JOIN sys.schemas s
    ON t.schema_id = s.schema_id

    UNION

    SELECT
          v.name AS table_name
        , s.name AS schema_name
    FROM sys.views v
    INNER JOIN sys.schemas s
    ON v.schema_id = s.schema_id

    ORDER BY schema_name, table_name;''')

mssqlserver_connection = mssqlserver_engine.connect()

mssqlserver_tables = mssqlserver_connection.execute(mssqlserver_table_query)
mssqlserver_tables = mssqlserver_tables.fetchall()
mssqlserver_tables = dict(mssqlserver_tables)

mssqlserver_schemas = set(mssqlserver_tables.values())

mssqlserver_connection.close()

################################################################
print(f"Tables collected. Found {len(mssqlserver_tables)} tables in {len(mssqlserver_schemas)} schemas.")
print()
print()
################################################################


### Schema creation

postgres_connection = postgres_engine.connect()

for schema in mssqlserver_schemas:
    schema_create = f"""

        DROP SCHEMA IF EXISTS "{schema.lower()}" CASCADE;
        CREATE SCHEMA "{schema.lower()}";
    """
    
    print(f"Executing SQL for schema creation: {schema_create}")
    postgres_connection.execute(text(schema_create))
    postgres_connection.commit() 
    print(f" - Schema {schema.lower()} created")

postgres_connection.close()


################################################################
print()
print(f"Schemas created.")
print()
print()
################################################################

def cast_unsupported_types(table_name, schema_name):
    columns_query = text(f'''
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema_name}';
    ''')
    mssqlserver_connection = mssqlserver_engine.connect()
    columns = mssqlserver_connection.execute(columns_query).fetchall()
    mssqlserver_connection.close()

    select_clause = []
    for column in columns:
        if column.data_type in ['geometry', 'geography']:
            select_clause.append(f"CAST([{column.column_name}] AS VARCHAR(MAX)) AS [{column.column_name}]")
        else:
            select_clause.append(f"[{column.column_name}]")
    
    return f"SELECT {', '.join(select_clause)} FROM [{schema_name}].[{table_name}]"

### Table dump

for table_name, schema_name in mssqlserver_tables.items():
    
    table_no = list(mssqlserver_tables.keys()).index(f"{table_name}") + 1
    ################################################################
    print()
    print(f"##### Dumping table No. {table_no} from {len(mssqlserver_tables)}: {schema_name}.{table_name}...")
    ################################################################
    
    mssqlserver_connection = mssqlserver_engine.connect()
    postgres_connection = postgres_engine.connect()
    
    table_split = [t for t in re.split("([A-Z][^A-Z]*)", table_name) if t]
    table_split = '_'.join(table_split)
    table_split = table_split.lower()
    
    ################################################################
    print(f"    . Converted {table_name} to --> {table_split}")
    ################################################################
    
    full_table = f"""

        SELECT
        *
        FROM {schema_name}.{table_name};

    """


    
    df = pd.read_sql(full_table, mssqlserver_connection)
    df.columns = map(str.lower, df.columns)
    df.to_sql(schema=schema_name.lower(), name=table_split, con=postgres_connection, chunksize=5000, index=False, index_label=False, if_exists='replace')
    
    ################################################################
    print(f"   .. Wrote {schema_name}.{table_split} to database")
    ################################################################
    
    
    postgres_connection.close()
    mssqlserver_connection.close()


mssqlserver_engine.dispose()
postgres_engine.dispose()


print()
print()
print("Engines disposed")
print()
print()
print("#################################### EL-PROCESS FINISHED ####################################")
print()
