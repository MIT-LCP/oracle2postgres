# Import libraries
import pandas as pd
import sqlalchemy
import cx_Oracle

# Import postgres types
from sqlalchemy.dialects.postgresql import \
    ARRAY, BIGINT, BIT, BOOLEAN, BYTEA, CHAR, CIDR, DATE, \
    DOUBLE_PRECISION, ENUM, FLOAT, HSTORE, INET, INTEGER, \
    INTERVAL, JSON, JSONB, MACADDR, NUMERIC, OID, REAL, SMALLINT, TEXT, \
    TIME, TIMESTAMP, UUID, VARCHAR, INT4RANGE, INT8RANGE, NUMRANGE, \
    DATERANGE, TSRANGE, TSTZRANGE, TSVECTOR

def drop_connections(dbname,engine):
    con = engine.connect()
    con.execute("COMMIT") # need to close current transaction
    con.execute("""
        SELECT pg_terminate_backend(pid) 
        FROM pg_stat_activity 
        WHERE datname = '{}';""".format(dbname))
    con.execute("COMMIT") # need to close current transaction
    con.close()

def drop_database(dbname,engine):
    con = engine.connect()
    con.execute("COMMIT") # need to close current transaction
    con.execute("DROP DATABASE IF EXISTS {}".format(dbname))
    con.execute("COMMIT") # need to close current transaction
    con.close()

def create_database(dbname,engine):
    con = engine.connect()
    con.execute("COMMIT") # need to close current transaction
    con.execute("CREATE DATABASE {}".format(dbname))
    con.execute("COMMIT") 
    con.close()

def check_default(default):
    new_default = default
    print("CHECKING DEFAULT")

    if str(default).lower() == "sysdate":
        print("UPDATED")
        new_default = None
    
    return new_default

def insert_data(target_engine,source_schema,table,data):
    """
    Inserts the data into the target system. Disables integrity checks 
    prior to inserting.
    """
    if data:
        with target_engine.begin() as connection:
            # disable integrity checks
            connection.execute("SET session_replication_role = replica;")
            # insert data
            connection.execute(table.insert(),data)
            # enable integrity checks
            connection.execute("SET session_replication_role = DEFAULT;")

def copy_data(source_engine,source_schema,target_engine,table,
    chunksize=10000,logged=True,debug=False):
    """
    Copies the data into the target system. Disables integrity checks 
    prior to inserting.
    """
    # print schema
    if debug:
        print(source_schema)

    # switch off logging
    if not logged:
        target_engine.execute('ALTER TABLE "{}" SET UNLOGGED'.format(table.name))

    # get the initial data chunk
    offset = 0
    query =  """SELECT * 
                FROM {}.{} 
                ORDER BY rowid 
                OFFSET {} ROWS 
                FETCH NEXT {} ROWS ONLY""".format(source_schema,table.name,offset,chunksize)
    data = source_engine.execute(query).fetchall()

    while data:
        # insert the data
        insert_data(target_engine,source_schema,table,data)

        # update the offset
        offset = offset + chunksize
        query =  """SELECT * 
                    FROM {}.{} 
                    ORDER BY rowid 
                    OFFSET {} ROWS 
                    FETCH NEXT {} ROWS ONLY""".format(source_schema,table.name,offset,chunksize)
        
        # load the next chunk of data
        data = target_engine.execute(query).fetchall()

        # print summary
        if debug:
            print('Copied table "{}" with offset({})'.format(table.name,offset))
            #break after a couple of loops
            if offset > 200:
                break

    # switch on logging
    if not logged:
        source_engine.execute('ALTER TABLE "{}" SET LOGGED'.format(table.name))

def convert_type(colname, ora_type):
    """
    Converts oracle type to Postgres type
    """
    pg_type = ora_type
    
    # "NullType is used as a default type for those cases 
    # where a type cannot be determined"
    # NB: this needs to be first in the list
    # Otherwise str(ora_type) clauses will error
    if isinstance(ora_type,sqlalchemy.types.NullType):
        pg_type = sqlalchemy.types.String()
        print('\t{}: NULL DETECTED'.format(colname))
        return pg_type
    elif isinstance(ora_type,sqlalchemy.types.Numeric):
        pg_type = sqlalchemy.types.Numeric()
    elif isinstance(ora_type,sqlalchemy.types.DateTime):
        pg_type = TIMESTAMP()
    # elif str(ora_type) == str(sqlalchemy.types.VARCHAR(1)):
    #     pg_type = sqlalchemy.types.VARCHAR()
    elif isinstance(ora_type,sqlalchemy.types.Text):
        pg_type = sqlalchemy.types.Text()
    elif isinstance(ora_type,sqlalchemy.types.NVARCHAR):
        pg_type = sqlalchemy.types.VARCHAR()
    elif isinstance(ora_type,sqlalchemy.types.BLOB):
        pg_type = BYTEA()
    elif str(ora_type) == 'RAW':
        pg_type = BYTEA()
    # this isn't currently catching the binary_float
    elif str(ora_type) == 'BINARY_FLOAT':
        pg_type = REAL()
    elif str(ora_type) == 'INTERVAL DAY TO SECOND':
        pg_type = sqlalchemy.types.Interval(second_precision=True)
    else:
        pass

    if pg_type != ora_type:
        print("\t{}: {} converted to {}".format(colname,ora_type,pg_type))

    return pg_type

