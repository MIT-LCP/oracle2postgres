# Import libraries
import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import cx_Oracle
import pdb

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

def insert_data(target_session,source_schema,table,data):
    """
    Inserts the data into the target system. Disables integrity checks 
    prior to inserting.
    """
    if data:
        # disable integrity checks
        target_session.execute("SET session_replication_role = replica;")
        # insert data
        target_session.execute(table.insert(),data)
        # enable integrity checks
        target_session.execute("SET session_replication_role = DEFAULT;")
        target_session.commit()

def get_column_string(table):
    """
    Creates a string of column names for including in a query.
    """
    column_list = table.columns.keys()

    # quote columns that are also keywords. assume they are upper case!
    keywords = ['where','from','select','comment']
    column_list = ['"{}"'.format(x.upper()) if x.lower() in keywords else x for x in column_list] 
    
    column_str = ', '.join(column_list)
    
    return column_str

def copy_data(source_engine,source_schema,target_engine,table,
    chunksize=10000,logged=True,verbose=True,trialrun=False):
    """
    Copies the data into the target system. Disables integrity checks 
    prior to inserting.
    """
    # Create sessions
    SourceSession = sessionmaker(bind=source_engine)
    source_session = SourceSession()
    TargetSession = sessionmaker(bind=target_engine)
    target_session = TargetSession()

    # print schema
    if verbose:
        print('Copying {}.{}'.format(source_schema,table.name))

    # switch off logging
    if not logged:
        target_session.execute('ALTER TABLE "{}" SET UNLOGGED'.format(table.name))

    columns = get_column_string(table)

    # get the initial data chunk
    offset = 0
    query =  """SELECT {} 
                FROM {}.{} 
                ORDER BY rowid 
                OFFSET {} ROWS 
                FETCH NEXT {} ROWS ONLY""".format(columns,source_schema,table.name,offset,chunksize)
    data = source_session.execute(query).fetchall()

    while data:
        # insert the data
        insert_data(target_session,source_schema,table,data)

        # print summary
        if verbose:
            print('    Copied rows {}-{}'.format(offset,offset+chunksize))
        
        # break after a couple of loops
        if trialrun and offset > 200:
            break

        # update the offset
        offset = offset + chunksize
        query =  """SELECT {} 
                    FROM {}.{} 
                    ORDER BY rowid 
                    OFFSET {} ROWS 
                    FETCH NEXT {} ROWS ONLY""".format(columns,source_schema,table.name,offset,chunksize)
        
        # load the next chunk of data
        try: 
            data = source_session.execute(query).fetchall()
        except:
            # break if end of table is reached
            data = None
            break

    # switch on logging
    if not logged:
        target_session.execute('ALTER TABLE "{}" SET LOGGED'.format(table.name))

    # close the sessions
    source_session.close()
    target_session.close()

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
