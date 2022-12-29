# Importing dependencies
import os
import sys
import pandas as pd
from main import conn, cursor
from .etl_tools import find_file

# EXTRACTION (Finding Data) and TRANSFORMATIONS


def extraction_terminals(date: str, cur_dir: str = "./"):
    """
    - Extraction data: terminals
    """

    # the input file with variable date
    df = find_file(cur_dir, f"terminals_{date}", ".xlsx")

    # read xlsx to dataframe
    df = pd.read_excel(df)

    # use pandas to load xlsx converted to DF into database
    df.to_sql("STG_TERMINALS", conn, if_exists="replace", index=False)

    return df


def extraction_transformation_transactions(date: str, cur_dir: str = "./"):
    """
    - Extraction data: transactions
    - Transformation data: transactions
    """

    # the input file with variable date
    file = find_file(cur_dir, f"transactions_{date}", ".txt")

    # read txt to dataframe
    df = pd.read_csv(file, sep=";", encoding="utf-8")

    # rename columns
    df.rename(columns={
        "transaction_id": "trans_id",
        "transaction_date": "trans_date",
        "amount": "amt"
    }, inplace=True)

    # convert 'trans_date' column from string to date type for decrease size dataframe
    df["trans_date"] = pd.to_datetime(df.trans_date, format="%Y-%m-%d %H:%M:%S")

    # use pandas to load txt converted to DF into database
    df.to_sql("STG_TRANSACTIONS", conn, if_exists="replace", index=False)

    return df


def extraction_transformation_passport_blacklist(date: str, cur_dir: str = "./"):
    """
    - Extraction data: passport_blacklist
    - Transformation data: passport_blacklist
    """

    # the input file with variable date
    file = find_file(cur_dir, f"passport_blacklist_{date}", ".xlsx")

    # read xlsx to dataframe
    df = pd.read_excel(file)

    # rename columns
    df.rename(columns={
        "passport": "passport_num",
        "date": "entry_dt"
    }, inplace=True)

    # convert 'date' column from string to date type
    df['entry_dt'] = pd.to_datetime(df.entry_dt, format="%d.%m.%y").dt.date

    # use pandas to load xlsx converted to DF into database
    df.to_sql("STG_PASSPORT_BLACKLIST", conn, if_exists="replace", index=False)

    return df


def load_sql_db(filename: str, cur_dir: str = "./sql_scripts"):
    """
    - Load sql files
    """

    # the input file
    file = find_file(cur_dir, filename, ".sql")

    # open and read the file
    with open(file, "r", encoding="utf-8") as f:
        # execute every command from the input file
        cursor.executescript(f.read())
        # saves all the modifications
        conn.commit()


def extraction_transformation_data(date: str):
    """
    - Load Data: ddl_dml, etl_schema, terminals, transactions
    """

    # cards, accounts, clients
    load_sql_db("ddl_dml", "./")

    # terminals
    extraction_terminals(date)

    # passport_blacklist
    extraction_transformation_passport_blacklist(date)

    # transactions
    extraction_transformation_transactions(date)
