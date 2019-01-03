#!/usr/bin/env python3

"""
Migrates data from source databases on an Oracle system to target schemas on
a Postgres system. 

Running the script will delete the target database before recreating it, so
use with caution!
"""
import sys
import migrate

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
        sys.exit()

    # get settings for migration
    migration_config = migrate.get_migration_config()
    source_config = migrate.get_source_config()
    target_config = migrate.get_target_config()

    # check the schema exist on the source database
    source_engine = migrate.connect_to_source(source_config)
    migrate.check_schema_exist(source_engine,source_config['schema_list'])

    # check and remove null characters in strings
    migrate.check_for_nulls(source_engine,source_config['schema_list'],remove=True)

    # create a new database on the target
    # WARNING: deletes target database before creation!
    target_engine = migrate.connect_to_target(target_config)
    migrate.drop_connections(target_config['database'],target_engine)
    migrate.drop_database(target_config['database'],target_engine)
    migrate.create_database(target_config['database'],target_engine)

    # create the schema on the target database
    target_engine = migrate.connect_to_target(target_config,target_config['database'])
    migrate.create_target_schema(source_config['schema_list'],source_engine,target_engine)

    # run the migration
    migrate.migrate(source_config,target_config,migration_config)

if __name__ == "__main__":
    """
    Execute when run as script
    """
    main()
