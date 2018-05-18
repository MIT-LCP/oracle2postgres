# Oracle to Postgres

This repository contains code that migrates schemas from Oracle to Postgres.
The code uses SQLAlchemy as an intermediary to map data types between the two database systems.

## Example Notebook

For example usage, see the Jupyter Notebook at: [https://github.com/MIT-LCP/oracle-to-postgres/blob/master/migrate_data.ipynb](https://github.com/MIT-LCP/oracle-to-postgres/blob/master/migrate_data.ipynb).

## Instructions for use

1. Clone the repository to your server: `git clone git@github.com:MIT-LCP/oracle-to-postgres.git`.
2. Run the `run_migration.py` script with `python run_migration.py`.
3. Follow the instructions to add details of the source and target databases.