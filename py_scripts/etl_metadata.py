# Importing dependencies
from main import conn, cursor


def update_metadata():
    """
    - Update META_TABLE
    """

    cursor.execute(
        """
        UPDATE
            META_TABLE
        SET
            tbl_name = (
                SELECT
                    tbl_name
                FROM
                    SQLITE_MASTER
                WHERE
                    tbl_name = META_TABLE.tbl_name
            ),
            last_update = CURRENT_TIMESTAMP
        WHERE
            EXISTS (
                SELECT
                    tbl_name
                FROM
                    SQLITE_MASTER
                WHERE tbl_name = META_TABLE.tbl_name
            )
    """
    )
