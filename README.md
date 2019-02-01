# Oracle to Postgres

This repository contains code that migrates schemas from Oracle to Postgres.
The code uses SQLAlchemy as an intermediary to map data types between the two database systems.

## Example Notebook

For example usage, see the Jupyter Notebook at: [https://github.com/MIT-LCP/oracle-to-postgres/blob/master/migrate_data.ipynb](https://github.com/MIT-LCP/oracle-to-postgres/blob/master/migrate_data.ipynb).

## Instructions for use

1. Clone the repository to your server: `git clone git@github.com:MIT-LCP/oracle-to-postgres.git`.
2. At the moment, you will need to manually update `schema_list = ['']` in `run_migration.py` with a list of schema to migrate.
3. Run the `run_migration.py` script with `python run_migration.py`.
4. Follow the instructions to add details of the source and target databases.

## Instructions for installing Oracle drivers

1. Set-up instructions https://oracle.github.io/odpi/doc/installation.html#linux
2. Go to oracle.com and download the instantclient version "Basic: All files required to run OCI, OCCI, and JDBC-OCI applications"
3. Install the `libaio` library
3. Add the libraries to the `LD_LIBRARY_PATH`
   * e.g. add to .bashrc as `export LD_LIBRARY_PATH=/opt/instantclient_12_2`
4. Add tnsnames.ora to the `/opt/instantclient_12_2/network/admin` folder