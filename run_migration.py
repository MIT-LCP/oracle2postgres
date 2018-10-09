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
import multiprocessing

def get_migration_config():
    """
    Get migration settings
    """
    print('\n--------------------------------------')
    print('Enter data migration settings:')
    print('-------------------------------------')

    config = {}

    # run in trial mode
    trialrun = input("- Run in trial mode (copy ~200 rows for each table), y or n (default 'n'): ") or "n"
    if trialrun.lower() == "y":
        config['trialrun'] = True
    else:
        config['trialrun'] = False

    # max size of migration chunk
    config['chunksize'] = int(input("- Maximum number of rows per chunk (default '300000', '100' in trial mode): ") or 300000)
    if config['trialrun']:
        config['chunksize'] = min(config['chunksize'],100)

    # disable logging for faster migration
    disable_log = input('- Disable logging (requires Postgres 9.5 or later), y or n (default "y"): ') or "y"
    if disable_log.lower() == "y":
        config['logged'] = False
    else:
        config['logged'] = True

    # run with multiprocessing
    multiprocess = input("- Run multiple processes, y or n (default 'n'): ") or "n"
    if multiprocess.lower() == "y":
        config['multiprocess'] = True
        processes = input("- Number of processes (leave empty to assign automatically): ") or None
        if processes:
            pool = multiprocessing.Pool(int(processes))
        else: 
            pool = multiprocessing.Pool()
    else:
        config['multiprocess'] = False
        pool = None

    return config, pool

def get_source_config():
    """
    Get details of the source database (Oracle)
    """
    print('\n------------------------------------------')
    print('Enter source database settings:')
    print('------------------------------------------')

    config = {}
    config['username'] = input('- Username on source database (default "sys"): ') or 'sys'
    config['host'] = input('- Hostname for source database (default "localhost": ') or 'localhost'
    config['port'] = input('- Port for source database (default "1521"): ') or 1521
    config['database'] = input('- Name of source database (default "sys"): ') or 'sys'
    config['password'] = getpass.getpass('- Password for source database: ')

    print('\nUsername: {}'.format(config['username']))
    print('Hostname: {}'.format(config['host']))
    print('Port: {}'.format(config['port']))
    print('Database name: {}'.format(config['database']))
    print('Password: {}'.format('*'*len(config['password'])))

    return config

def get_target_config():
    """ 
    Get details of the target database (Postgres)
    """
    print('\n------------------------------------------')
    print('Enter target database settings:')
    print('------------------------------------------')

    config = {}
    config['username'] = input('- Username on target database (default "postgres"): ') or 'postgres'
    config['host'] = input('- Hostname for target database (default "localhost"): ') or 'localhost'
    config['port'] = input('- Port for target database (default "5432"): ') or 5432
    config['database'] = input("- Name of target database (default 'oracle_migration'): ") or "oracle_migration"
    config['password'] = getpass.getpass('- Password for target database: ')

    print('\nUsername: {}'.format(config['username']))
    print('Hostname: {}'.format(config['host']))
    print('Port: {}'.format(config['port']))
    print('Database name: {}'.format(config['database']))
    print('Password: {}'.format('*'*len(config['password'])))

    return config

def connect_to_source(config):
    """
    Connect to source database
    """
    print_log = False

    dsn_str = cx_Oracle.makedsn(config['host'],config['port'],service_name=config['database'])
    con_string = 'oracle://{}:{}@'.format(config['username'], config['password']) + dsn_str
    engine = sqlalchemy.create_engine(con_string, echo = print_log)

    return engine

def connect_to_target(config,dbname=None):
    """
    Connect to target database
    """
    print_log = False

    if dbname:
        con_string = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(config['username'], 
            config['password'], config['host'], config['port'], dbname)
    else:
        con_string = 'postgresql+psycopg2://{}:{}@{}:{}'.format(config['username'], 
            config['password'], config['host'], config['port'])

    engine = sqlalchemy.create_engine(con_string, echo = print_log)

    return engine

def get_schema_list():
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

def migrate_data(schema,source_config,target_config,migration_config):
    """
    Migrate the data from the source tables to the target tables
    """
    # create database connections
    source_engine = connect_to_source(source_config)
    target_engine = connect_to_target(target_config,target_config['database'])
    
    # load the schema metadata profile
    source_metadata = sqlalchemy.MetaData(source_engine)
    source_metadata.reflect(schema=schema)

    # iterate the tables, loading the data
    for t in source_metadata.sorted_tables:
        migrate.copy_data(source_engine,schema,target_engine,t,migration_config['chunksize'],
            migration_config['logged'],trialrun=migration_config['trialrun'])

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

    if input(msg).lower() != "y":
        exit()

    # get migration settings
    migration_config, pool = get_migration_config()
    source_config = get_source_config()
    target_config = get_target_config()

    # get the list of schema to copy
    schema_list = get_schema_list()

    # create a new database on the target
    # WARNING: deletes target database before creation!
    target_engine = connect_to_target(target_config)
    migrate.drop_connections(target_config['database'],target_engine)
    migrate.drop_database(target_config['database'],target_engine)
    migrate.create_database(target_config['database'],target_engine)

    # create the schema on the target database
    source_engine = connect_to_source(source_config)
    target_engine = connect_to_target(target_config,target_config['database'])
    create_target_schema(schema_list,source_engine,target_engine)

    # migrate the data
    print('Migrating data to target database...\n')

    # set up multiprocessing
    if migration_config['multiprocess']:
        # starmap takes an iterable list
        arg_iterable = [[schema,source_config,target_config,migration_config] for schema in schema_list]
        pool.starmap(migrate_data,arg_iterable)
    else:
        for schema in schema_list:
            migrate_data(schema,source_config,target_config,migration_config)

    print('Migration complete!\n')

if __name__ == "__main__":
    """
    Execute when run as script
    """
    main()

