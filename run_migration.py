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


def get_source_details():
    """
    Get details of the source database (Oracle)
    """
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

    return src_username,src_host,src_port,src_database,src_password

def get_target_details():
    """ 
    Get details of the target database (Postgres)
    """
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

    return target_username,target_host,target_port,target_database,target_password


def connect_to_source(src_username,src_host,src_port,src_database,src_password):
    """
    Connect to source database
    """
    dsn_str = cx_Oracle.makedsn(src_host,src_port,service_name=src_database)
    src_con_string = 'oracle://{}:{}@'.format(src_username, src_password) + dsn_str
    source_engine = sqlalchemy.create_engine(src_con_string)

    return source_engine

def connect_to_target(target_username,target_host,target_port,target_database,target_password):
    """
    Connect to target database
    """
    print_log = False
    target_con_string = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(target_username, 
        target_password, target_host, target_port, target_database)
    target_engine = sqlalchemy.create_engine(target_con_string, echo = print_log)

    return target_engine

def get_migration_settings():
    """
    Get migration settings
    """
    print('\n--------------------------------------')
    print('Enter data migration settings:')
    print('-------------------------------------')

    chunksize = int(input('- Maximum number of rows per chunk (default 300000): ') or 300000)
    trialrun = input('- Run in trial mode (copy ~200 rows for each table), y or n (default "n"): ') or "n"
    if trialrun == "y":
        trialrun = True
    else:
        trialrun = False
    disable_log = input('- Disable logging (requires Postgres 9.5 or later), y or n (default "y"): ') or "y"
    if disable_log == "y":
        logged = False
    else:
        logged = True

    return chunksize,logged,trialrun

def get_list_of_schema():
    """
    Get list of schema to migrate
    """

    schema_list = ['']

    ## get list of all schema
    # inspector = sqlalchemy.inspect(source_engine)
    # schema_list = inspector.get_schema_names()
    
    print('\nList of schema to copy: {}\n'.format(schema_list))

    return schema_list

def create_target_schema(schema_list,source_engine,target_engine):
    """
    Recreate the sources tables on the target database
    """
    print('Creating schema on target database...\n')
    for source_schema in schema_list:
        
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

def copy_data(schema_list,source_engine,target_engine,chunksize,logged,trialrun):
    """
    Migrate the data from the source tables to the target tables
    """
    print('Migrating data to target database...\n')
    for source_schema in schema_list:
        
        # load the schema metadata profile
        source_metadata = sqlalchemy.MetaData(source_engine)
        source_metadata.reflect(schema=source_schema)

        # iterate the tables, loading the data
        for t in source_metadata.sorted_tables:
            migrate.copy_data(source_engine,source_schema,target_engine,t,chunksize,
                logged,trialrun=trialrun)

def main():
    """
    Connects to the source and target databases, then migrates a list of defined schema.
    """
    msg =  """
            ----------------------------------------------------- \n
            Running this script will delete the target database!  \n
            And it will close connections on the target database. \n
            Are you sure you wish to continue? (y/n)              \n
            ----------------------------------------------------- \n
            \n"""
    if input(msg) != "y":
        exit()

    src_username,src_host,src_port,src_database,src_password = get_source_details()
    source_engine = connect_to_source(src_username,src_host,src_port,src_database,
        src_password)

    target_username,target_host,target_port,target_database,target_password = get_target_details()
    target_engine = connect_to_target(target_username,target_host,target_port,
        target_database,target_password)

    chunksize,logged,trialrun = get_migration_settings()

    # create a new database on the target
    # WARNING: deletes target database before creation!
    target_database_new = 'oracle_migration'
    migrate.drop_connections(target_database_new,target_engine)
    migrate.drop_database(target_database_new,target_engine)
    migrate.create_database(target_database_new,target_engine)

    # reconnect to the target database
    target_engine = connect_to_target(target_username,target_host,target_port,
        target_database_new,target_password)

    # define the list of schema to copy
    schema_list = get_list_of_schema()

    create_target_schema(schema_list,source_engine,target_engine)
    copy_data(schema_list,source_engine,target_engine,chunksize,logged,trialrun)
    print('Migration complete!\n')

if __name__ == "__main__":
    """
    Execute when run as script
    """
    main()

