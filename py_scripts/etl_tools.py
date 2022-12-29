# Importing dependencies
import os
import re
import shutil
from main import conn, cursor


def find_file(cur_dir: str, filename: str, filename_extension: str):
    """
    - Search for a file in directory
    - cur_dir: dir from where search starts
    """

    for file in os.listdir(cur_dir):
        if file.startswith(filename) and file.endswith(filename_extension):
            # concatenates various path components
            return os.path.join(cur_dir, file)


def get_date_default(date: str) -> str:
    """
    - Get default to the date
    """

    # extract current date from given files names in data folder
    if date == '' or date == ' ':
        for file in sorted(os.listdir(os.getcwd())):
            if file.startswith('terminals'):
                regex = re.compile(r"\d{8}")  # pattern to capture date
                match_array = regex.findall(file)
                date = match_array[0]
                return date
    else:
        # date format to use for files
        return re.sub(r"(\d\d)\W?(\d\d)\W?(\d{4})", r"\1\2\3", date)


def get_next_file(file_name: str):
    """
    - Prevent shutil.move from overwriting a file if it already exists
    - backup: to name a backup file
    - dest: create our new destination
    """

    backup = f"{file_name}.backup"
    dest = os.path.join("archive", backup)

    # loop until we find a file that doesn't exist
    num = 0
    while os.path.exists(dest):
        num += 1
        # use rfind to find your file extension if there is one
        period = backup.rfind('.')
        # this ensures that it will work with files without extensions
        if period == -1:
            period = len(backup)

        # we could extract the number and increment it, but this allows us to fill in the gaps if there are any
        # it has the added benefit of avoiding errors in file names like this "test(sometext).txt"
        new_file = f"{backup[:period]}({num}){backup[period:]}"

        dest = os.path.join("archive", new_file)

    return dest


def make_backup_file(filename: str, filename_extension: str, cur_dir: str = "./"):
    """
    - Make a backup copy of a file
    """

    # find source file
    source = find_file(cur_dir, filename, filename_extension)

    # prevent overwriting a file
    backup = get_next_file(filename)

    # shutil.move() first copies the source to destination and then removes the source
    shutil.move(source, backup)


def make_backup_all_files(date: str):
    """
    - Create a backup copy of a files
    """

    make_backup_file(f"passport_blacklist_{date}", ".xlsx")
    make_backup_file(f"terminals_{date}", ".xlsx")
    make_backup_file(f"transactions_{date}", ".txt")


def count_rows(table_name: str):
    """
    - Print the total number of records
    """

    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    results = cursor.fetchone()[0]
    print(f"{results} rows loaded")


def show_table(table_name: str):
    """
    - Print out a table with all the data with the column names
    """

    cursor.execute(f"SELECT * FROM {table_name}")
    results = cursor.fetchall()

    # get column names
    field_names = [i[0] for i in cursor.description]
    print(field_names)

    # print out a table with all the data
    for row in results:
        print(row)

    # print total number of rows
    count_rows(table_name)


def show_all_tables():
    """
    - Check the tables
    """

    show_table("META_TABLE")
    show_table("DWH_DIM_CARDS_HIST")
    show_table("DWH_DIM_ACCOUNTS_HIST")
    show_table("DWH_DIM_CLIENTS_HIST")
    show_table("DWH_DIM_TERMINALS_HIST")
    show_table("DWH_FACT_PASSPORT_BLACKLIST")
    show_table("DWH_FACT_TRANSACTIONS")
    show_table("REP_FRAUD")
