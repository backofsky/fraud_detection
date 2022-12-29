# Import dependencies
import sqlite3
import sys
import py_scripts as ps


# connect to default database.db
with sqlite3.connect("database.db") as conn:
    # establish connection
    cursor = conn.cursor()


def get_date():
    """
    Get date
    """

    # format date: ddmmyyyy, dd.mm.yyyy, dd/mm/yyyy
    date = ''

    # date
    date = ps.get_date_default(date)

    # if date not extracted
    if date is None:
        print("files not found")
        sys.exit()
    else:
        return date


def execute_etl():
    """
    - EXTRACTION, TRANSFORMATIONS and LOADING
    """

    try:
        # clear data from STG
        ps.load_sql_db("etl_stg_drop")
        print('drop STG tables: ok')

        # date
        date = get_date()
        print(f"date: {date}")

        # creates each table
        ps.load_sql_db("etl_schema")
        print("creates DWH tables: ok")

        # extraction and transformation
        ps.extraction_transformation_data(date)
    except (FileNotFoundError, BaseException) as e:
        print(e)
        sys.exit()

    print("extraction(finding data) and transformation: ok")

    ps.load_dim_fact_tables()
    print("loading tables: ok")

    ps.build_data_mart(date)
    print("build report: ok")

    ps.update_metadata()
    print("update metadata: ok")

    print("report info:")
    ps.show_table("REP_FRAUD")

    # check the tables
    # ps.show_all_tables()

    # clear data
    ps.load_sql_db("etl_stg_drop")

    ps.make_backup_all_files(date)
    print('backup files: ok')


if __name__ == '__main__':
    execute_etl()
