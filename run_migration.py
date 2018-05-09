#!/usr/bin/env python3

"""
Migrates data from source databases on an Oracle system to target schemas on
a Postgres system. 

Running the script will delete the target database before recreating it, so
use with caution!
"""
# Import libraries
import sqlalchemy
import cx_Oracle
import getpass
import migrate
import psycopg2

msg =  """
        ----------------------------------------------------
        Running this script will delete the target database!\n
        Are you sure you wish to continue? (y/n)
        ----------------------------------------------------
        \n"""
if input(msg) != "y":
    exit()

# connect to source database
print('\n------------------------------------------')
print('Enter source database settings:')
print('------------------------------------------')
src_username = input('- Username on source database (default "sys"): ') or 'sys'
src_host = input('- Hostname for source database (default "localhost": ') or 'localhost'
src_port = input('- Port for source database (default "1521"): ') or 1521
src_database = input('- Name of source database (default "sys"): ') or 'sys'
src_password = getpass.getpass('- Password for source database: ')

print('\nUsername: {}'.format(src_username))
print('Hostname: {}'.format(src_host))
print('Port: {}'.format(src_port))
print('Database name: {}'.format(src_database))
print('Password: {}'.format('*'*len(src_password)))

dsn_str = cx_Oracle.makedsn(src_host,src_port,service_name=src_database)
src_con_string = 'oracle://{}:{}@'.format(src_username, src_password) + dsn_str
source_engine = sqlalchemy.create_engine(src_con_string)

# connect to target database
print('\n------------------------------------------')
print('Enter target database settings:')
print('------------------------------------------')
target_username = input('- Username on target database (default "postgres"): ') or 'postgres'
target_host = input('- Hostname for target database (default "localhost"): ') or 'localhost'
target_port = input('- Port for target database (default "5432"): ') or 5432
target_database = input('- Name of target database (default "postgres"): ') or 'postgres'
target_password = getpass.getpass('- Password for target database: ')

print('\nUsername: {}'.format(target_username))
print('Hostname: {}'.format(target_host))
print('Port: {}'.format(target_port))
print('Database name: {}'.format(target_database))
print('Password: {}'.format('*'*len(target_password)))

print_log = False
target_con_string = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(target_username, 
    target_password, target_host, target_port, target_database)
target_engine = sqlalchemy.create_engine(target_con_string, echo = print_log)

print('\n--------------------------------------')
print('Enter data migration settings:')
print('-------------------------------------')

chunksize = int(input('- Maximum number of rows per chunk (default 100000): ') or 100000)
debug = input('- Run in debug mode, y or n (default "n"): ') or "n"
if debug == "y":
    debug = True
else:
    debug = False

# create a new database on the target
# WARNING: deletes target database before creation!
target_database_new = 'oracle_migration'
migrate.drop_connections(target_database_new,target_engine)
migrate.drop_database(target_database_new,target_engine)
migrate.create_database(target_database_new,target_engine)

# reconnect to this target database
target_con_string2 = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(target_username, 
    target_password, target_host, target_port, target_database_new)
target_engine = sqlalchemy.create_engine(target_con_string2, echo = print_log)

# get list of all schema
inspector = sqlalchemy.inspect(source_engine)
schema_list = inspector.get_schema_names()
print('List of schema on source database: {}\n'.format(schema_list))

# Omit the following schema from the migration
omit= ['']

print('List of schema to be omitted from migration: {}\n'.format(omit))

# Recreate the sources tables on the target database
print('Creating schema on target database...\n')
for source_schema in schema_list:

    # skip schema in omit list
    if source_schema in omit:
        continue
    
    # load the schema metadata profile
    print(source_schema)
    source_metadata = sqlalchemy.MetaData(source_engine,quote_schema=True)
    source_metadata.reflect(schema=source_schema)

    # create the schema on the target database
    target_engine.execute(sqlalchemy.schema.CreateSchema(source_schema))

    # iterate the tables
    for t in source_metadata.sorted_tables:

        # clear the indexes and constraints
        t.indexes.clear()
        t.constraints.clear()
        
        # clean the data types
        for col in t.columns:
            
            # set the column types
            newtype = migrate.convert_type(col.name, col.type)
            t.c[col.name].type = newtype
            
            # check the default values
            if t.c[col.name].default:
                new_default = migrate.check_default(t.c[col.name].default)
                t.c[col.name].default = new_default

            # remove the server_default values
            if t.c[col.name].server_default:
                t.c[col.name].server_default = None            

    # Build the tables on the target database
    source_metadata.create_all(target_engine,checkfirst=False)

print('Target schema created!\n')

# Migrate the data from the source tables to the target tables
print('Migrating data to target database...\n')
for source_schema in schema_list:

    # skip schema in omit list
    if source_schema in omit:
        continue
    
    # load the schema metadata profile
    source_metadata = sqlalchemy.MetaData(source_engine)
    source_metadata.reflect(schema=source_schema)

    # iterate the tables, loading the data
    for t in source_metadata.sorted_tables:
        migrate.copy_data(source_engine,source_schema,target_engine,t,chunksize,debug)

print('Migration complete!\n')
