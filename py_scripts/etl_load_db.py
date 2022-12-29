# Importing dependencies
from main import conn, cursor

# CARDS

# LOADING SCD1 table: DWH_DIM_CARDS


def init_stg_cards():
    """
    - Create staging table: STG_CARDS
    - Staging Table has all required fields
    """

    cursor.execute(
        """
        INSERT INTO STG_CARDS (
            card_num,
            account_num,
            create_dt,
            update_dt
        ) SELECT
            card_num,
            account,
            create_dt,
            update_dt
        FROM
            CARDS
    """
    )


def create_not_matched_rows_cards():
    """
    - Create staging table: STG_NOT_MATCHED_ROWS_CARDS
    - Select the not matched records
    """

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS STG_NOT_MATCHED_ROWS_CARDS
        AS
        SELECT
            t1.card_num,
            t1.account_num,
            t1.create_dt,
            t1.update_dt
        FROM
            STG_CARDS t1
            LEFT JOIN DWH_DIM_CARDS t2 ON t1.card_num = t2.card_num
        WHERE
            t2.card_num IS NULL
    """
    )


def create_matched_rows_cards():
    """
    - Create staging tables: STG_MATCHED_ROWS_CARDS
    - Select the matched records
    """

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS STG_MATCHED_ROWS_CARDS
        AS
        SELECT
            t1.card_num,
            t1.account_num,
            t1.create_dt,
            t1.update_dt
        FROM 
            STG_CARDS t1
            INNER JOIN DWH_DIM_CARDS t2 ON t1.card_num = t2.card_num
            AND (
                t1.account_num = t2.account_num
                OR t1.create_dt = t2.create_dt
                OR t1.update_dt = t2.update_dt
            )
    """
    )


def update_cards_scd1():
    """
    - Insert the new records and update the existing records
    """

    # new records
    cursor.execute(
        """
        INSERT INTO DWH_DIM_CARDS (
            card_num,
            account_num,
            create_dt,
            update_dt
        ) SELECT
            card_num,
            account_num,
            create_dt,
            update_dt
        FROM
            STG_NOT_MATCHED_ROWS_CARDS
    """
    )

    # overwrite existing data
    cursor.execute(
        """
        UPDATE
            DWH_DIM_CARDS
        SET
            account_num = (
                SELECT
                    account_num
                FROM 
                    STG_MATCHED_ROWS_CARDS
                WHERE
                    card_num = DWH_DIM_CARDS.card_num
            ),
            create_dt = (
                SELECT
                    create_dt
                FROM 
                    STG_MATCHED_ROWS_CARDS
                WHERE
                    card_num = DWH_DIM_CARDS.card_num
            ),
            update_dt = (
                SELECT
                    update_dt
                FROM
                    STG_MATCHED_ROWS_CARDS
                WHERE
                    card_num = DWH_DIM_CARDS.card_num
            )
        WHERE
            EXISTS (
                SELECT
                    account_num
                FROM
                    STG_MATCHED_ROWS_CARDS
                WHERE
                    card_num = DWH_DIM_CARDS.card_num
            )
            OR EXISTS (
                SELECT
                    create_dt
                FROM
                    STG_MATCHED_ROWS_CARDS
                WHERE
                    card_num = DWH_DIM_CARDS.card_num
            )
            OR EXISTS (
                SELECT
                    update_dt
                FROM
                    STG_MATCHED_ROWS_CARDS
                WHERE
                    card_num = DWH_DIM_CARDS.card_num
            )
    """
    )

    # saves all the modifications
    conn.commit()

# LOADING SCD2 table: DWH_DIM_CARDS_HIST


def init_view_cards():
    """
    - Create staging view: STG_VIEW_CARDS
    """

    cursor.execute(
        """
        CREATE VIEW IF NOT EXISTS STG_VIEW_CARDS
        AS
        SELECT
            card_num,
            account_num
        FROM
            DWH_DIM_CARDS_HIST
        WHERE
            CURRENT_TIMESTAMP BETWEEN effective_from AND effective_to
            AND deleted_flg = 0
    """
    )


def create_new_rows_cards():
    """
    - Create staging table: STG_NEW_ROWS_CARDS
    - Select the new records
    """

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS STG_NEW_ROWS_CARDS
        AS
        SELECT
            t1.card_num,
            t1.account_num
        FROM
            DWH_DIM_CARDS t1
            LEFT JOIN STG_VIEW_CARDS t2 ON t1.card_num = t2.card_num
        WHERE
            t2.card_num IS NULL
    """
    )


def create_deleted_rows_cards():
    """
    - Create staging table: STG_DELETED_ROWS_CARDS
    - Soft delete the records
    """

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS STG_DELETED_ROWS_CARDS
        AS
        SELECT
            t1.card_num,
            t1.account_num
        FROM
            STG_VIEW_CARDS t1
            LEFT JOIN DWH_DIM_CARDS t2 ON t1.card_num = t2.card_num
        WHERE
            t2.card_num IS NULL
    """
    )


def create_changed_rows_cards():
    """
    - Create staging table: STG_CHANGED_ROWS_CARDS
    - Select changed rows
    """

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS STG_CHANGED_ROWS_CARDS
        AS
        SELECT
            t1.card_num,
            t1.account_num
        FROM 
            DWH_DIM_CARDS t1
            INNER JOIN STG_VIEW_CARDS t2 ON t1.card_num = t2.card_num
            AND t1.account_num <> t2.account_num
    """
    )


def update_cards_scd2():
    """
    - Add new row
    """

    # new records
    cursor.execute(
        """
        INSERT INTO DWH_DIM_CARDS_HIST (
            card_num,
            account_num
        ) SELECT
            card_num,
            account_num
        FROM
            STG_NEW_ROWS_CARDS
    """
    )

    # modified records
    cursor.execute(
        """
        UPDATE
            DWH_DIM_CARDS_HIST
        SET
            effective_to = DATETIME('now', '-1 second')
        WHERE
            card_num IN (
                SELECT
                    card_num
                FROM
                    STG_CHANGED_ROWS_CARDS
            )
            AND effective_to = DATETIME('2999-12-31 23:59:59')
    """
    )

    cursor.execute(
        """
        INSERT INTO DWH_DIM_CARDS_HIST (
            card_num,
            account_num
        ) SELECT
            card_num,
            account_num
        FROM
            STG_CHANGED_ROWS_CARDS
    """
    )

    # soft deleted records
    cursor.execute(
        """
        UPDATE
            DWH_DIM_CARDS_HIST
        SET
            effective_to = DATETIME('now', '-1 second')
        WHERE
            card_num IN (
                SELECT
                    card_num
                FROM
                    STG_DELETED_ROWS_CARDS
            )
            AND effective_to = DATETIME('2999-12-31 23:59:59')
    """
    )

    cursor.execute(
        """
        INSERT INTO DWH_DIM_CARDS_HIST (
            card_num,
            account_num,
            deleted_flg
        ) SELECT
            card_num,
            account_num,
            1
        FROM
            STG_DELETED_ROWS_CARDS
    """
    )

    # saves all the modifications
    conn.commit()

# ACCOUNTS

# LOADING SCD1 table: DWH_DIM_ACCOUNTS


def init_stg_accounts():
    """
    - Create staging table: STG_ACCOUNTS
    - Staging Table has all required fields
    """

    cursor.execute(
        """
        INSERT INTO STG_ACCOUNTS (
            account_num,
            valid_to,
            client,
            create_dt,
            update_dt
        ) SELECT
            account,
            valid_to,
            client,
            create_dt,
            update_dt
        FROM
            ACCOUNTS
    """
    )


def create_not_matched_rows_accounts():
    """
    - Create staging table: STG_NOT_MATCHED_ROWS_ACCOUNTS
    - Select the not matched records
    """

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS STG_NOT_MATCHED_ROWS_ACCOUNTS
        AS
        SELECT
            t1.account_num,
            t1.valid_to,
            t1.client,
            t1.create_dt,
            t1.update_dt
        FROM
            STG_ACCOUNTS t1
            LEFT JOIN DWH_DIM_ACCOUNTS t2 ON t1.account_num = t2.account_num
        WHERE
            t2.account_num IS NULL
    """
    )


def create_matched_rows_accounts():
    """
    - Create staging tables: STG_MATCHED_ROWS_ACCOUNTS
    - Select the matched records
    """

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS STG_MATCHED_ROWS_ACCOUNTS
        AS
        SELECT
            t1.account_num,
            t1.valid_to,
            t1.client,
            t1.create_dt,
            t1.update_dt
        FROM 
            STG_ACCOUNTS t1
            INNER JOIN DWH_DIM_ACCOUNTS t2 ON t1.account_num = t2.account_num
            AND (
                t1.client <> t2.client
                OR t1.valid_to <> t2.valid_to
                OR t1.create_dt <> t2.create_dt
                OR t1.update_dt <> t2.update_dt
                OR t1.account_num <> t2.account_num
            )
    """
    )


def update_accounts_scd1():
    """
    - Insert the new records and update the existing records
    """

    # new records
    cursor.execute(
        """
        INSERT INTO DWH_DIM_ACCOUNTS (
            account_num,
            valid_to,
            client,
            create_dt,
            update_dt
        ) SELECT
            account_num,
            valid_to,
            client,
            create_dt,
            update_dt
        FROM
            STG_NOT_MATCHED_ROWS_ACCOUNTS
    """
    )

    # overwrite existing rows
    cursor.execute(
        """
        UPDATE
            DWH_DIM_ACCOUNTS
        SET
            valid_to = (
                SELECT
                    valid_to
                FROM
                    STG_MATCHED_ROWS_ACCOUNTS
                WHERE
                    account_num = DWH_DIM_ACCOUNTS.account_num
            ),
            client = (
                SELECT
                    client
                FROM
                    STG_MATCHED_ROWS_ACCOUNTS
                WHERE
                    account_num = DWH_DIM_ACCOUNTS.account_num
                ),
            create_dt = (
                SELECT
                    create_dt
                FROM
                    STG_MATCHED_ROWS_ACCOUNTS
                WHERE
                    account_num = DWH_DIM_ACCOUNTS.account_num
                ),
            update_dt = (
                SELECT
                    update_dt
                FROM
                    STG_MATCHED_ROWS_ACCOUNTS
                WHERE
                    account_num = DWH_DIM_ACCOUNTS.account_num
                )
        WHERE
            EXISTS (
                SELECT
                    account_num
                FROM
                    STG_MATCHED_ROWS_ACCOUNTS
                WHERE
                    account_num = DWH_DIM_ACCOUNTS.account_num
                )
            OR EXISTS (
                SELECT
                    create_dt
                FROM
                    STG_MATCHED_ROWS_ACCOUNTS
                WHERE
                    account_num = DWH_DIM_ACCOUNTS.account_num
                )
            OR EXISTS (
                SELECT
                    update_dt
                FROM
                    STG_MATCHED_ROWS_ACCOUNTS
                WHERE
                    account_num = DWH_DIM_ACCOUNTS.account_num
                )
    """
    )

    # saves all the modifications
    conn.commit()

# LOADING SCD2 table: DWH_DIM_ACCOUNTS_HIST


def init_view_accounts():
    """
    - Create staging view: STG_VIEW_ACCOUNTS
    """

    cursor.execute(
        """
        CREATE VIEW IF NOT EXISTS STG_VIEW_ACCOUNTS
        AS
        SELECT
            account_num,
            valid_to,
            client
        FROM
            DWH_DIM_ACCOUNTS_HIST
        WHERE
            CURRENT_TIMESTAMP BETWEEN effective_from AND effective_to
            AND deleted_flg = 0
    """
    )


def create_new_rows_accounts():
    """
    - Create staging table: STG_NEW_ROWS_ACCOUNTS
    - Select the new records
    """

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS STG_NEW_ROWS_ACCOUNTS
        AS
        SELECT
            t1.account_num,
            t1.valid_to,
            t1.client
        FROM
            DWH_DIM_ACCOUNTS t1
            LEFT JOIN STG_VIEW_ACCOUNTS t2 ON t1.account_num = t2.account_num
        WHERE
            t2.account_num IS NULL
    """
    )


def create_deleted_rows_accounts():
    """
    - Create staging table: STG_DELETED_ROWS_ACCOUNTS
    - Soft delete the records
    """

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS STG_DELETED_ROWS_ACCOUNTS
        AS
        SELECT
            t1.account_num,
            t1.valid_to,
            t1.client
        FROM
            STG_VIEW_ACCOUNTS t1
            LEFT JOIN DWH_DIM_ACCOUNTS t2 ON t1.account_num = t2.account_num
        WHERE
            t2.account_num IS NULL
    """
    )


def create_changed_rows_accounts():
    """
    - Create staging table: STG_CHANGED_ROWS_ACCOUNTS
    - Select changed rows
    """

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS STG_CHANGED_ROWS_ACCOUNTS
        AS
        SELECT
            t1.account_num,
            t1.valid_to,
            t1.client
        FROM
            DWH_DIM_ACCOUNTS t1
            INNER JOIN STG_VIEW_ACCOUNTS t2 ON t1.account_num = t2.account_num
            AND (
                t1.valid_to <> t2.valid_to
                OR t1.client <> t2.client
            )
    """
    )


def update_accounts_scd2():
    """
    - Add new row
    """

    # new records
    cursor.execute(
        """
        INSERT INTO DWH_DIM_ACCOUNTS_HIST (
            account_num,
            valid_to,
            client
        ) SELECT
            account_num,
            valid_to,
            client
        FROM
            STG_NEW_ROWS_ACCOUNTS
    """
    )

    # modified records
    cursor.execute(
        """
        UPDATE
            DWH_DIM_ACCOUNTS_HIST
        SET
            effective_to = DATETIME('now', '-1 second')
        WHERE
            account_num IN (
                SELECT
                    account_num
                FROM
                    STG_CHANGED_ROWS_ACCOUNTS
            )
            AND effective_to = DATETIME('2999-12-31 23:59:59')
    """
    )

    cursor.execute(
        """
        INSERT INTO DWH_DIM_ACCOUNTS_HIST (
            account_num,
            valid_to,
            client
        ) SELECT
            account_num,
            valid_to,
            client
        FROM
            STG_CHANGED_ROWS_ACCOUNTS
    """
    )

    # soft deleted records
    cursor.execute(
        """
        UPDATE
            DWH_DIM_ACCOUNTS_HIST
        SET
            effective_to = DATETIME('now', '-1 second')
        WHERE
            account_num IN (
                SELECT
                    account_num
                FROM
                    STG_DELETED_ROWS_ACCOUNTS
            )
            AND effective_to = DATETIME('2999-12-31 23:59:59')
    """
    )

    cursor.execute(
        """
        INSERT INTO DWH_DIM_ACCOUNTS_HIST (
            account_num,
            valid_to,
            client,
            deleted_flg
        ) SELECT
            account_num,
            valid_to,
            client,
            1
        FROM
            STG_DELETED_ROWS_ACCOUNTS
    """
    )

    # saves all the modifications
    conn.commit()

# CLIENTS

# LOADING SCD1 table: DWH_DIM_CLIENTS


def init_stg_clients():
    """
    - Create staging table: STG_CLIENTS
    - Staging Table has all required fields
    """

    cursor.execute(
        """
        INSERT INTO STG_CLIENTS (
            client_id,
            last_name,
            first_name,
            patronymic,
            date_of_birth,
            passport_num,
            passport_valid_to,
            phone,
            create_dt,
            update_dt
        ) SELECT
            client_id,
            last_name,
            first_name,
            patronymic,
            date_of_birth,
            passport_num,
            passport_valid_to,
            phone,
            create_dt,
            update_dt
        FROM
            CLIENTS
    """
    )


def create_not_matched_rows_clients():
    """
    - Create staging tables: STG_NOT_MATCHED_ROWS_CLIENTS
    - Select the not matched records
    """

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS STG_NOT_MATCHED_ROWS_CLIENTS
        AS
        SELECT
            t1.client_id,
            t1.last_name,
            t1.first_name,
            t1.patronymic,
            t1.date_of_birth,
            t1.passport_num,
            t1.passport_valid_to,
            t1.phone,
            t1.create_dt,
            t1.update_dt
        FROM
            STG_CLIENTS t1
            LEFT JOIN DWH_DIM_CLIENTS t2 ON t1.client_id = t2.client_id
        WHERE
            t2.client_id IS NULL
    """
    )


def create_matched_rows_clients():
    """
    - Create staging tables: STG_MATCHED_ROWS_CLIENTS
    - Select the matched records
    """

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS STG_MATCHED_ROWS_CLIENTS
        AS
        SELECT
            t1.client_id,
            t1.last_name,
            t1.first_name,
            t1.patronymic,
            t1.date_of_birth,
            t1.passport_num,
            t1.passport_valid_to,
            t1.phone,
            t1.create_dt,
            t1.update_dt
        FROM
            STG_CLIENTS t1
            INNER JOIN DWH_DIM_CLIENTS t2 ON t1.client_id = t2.client_id
            AND (
                t1.phone = t2.phone
                OR t1.client_id = t2.client_id
                OR t1.last_name = t2.last_name
                OR t1.create_dt = t2.create_dt
                OR t1.update_dt = t2.update_dt
                OR t1.first_name = t2.first_name
                OR t1.patronymic = t2.patronymic
                OR t1.passport_num = t2.passport_num
                OR t1.date_of_birth = t2.date_of_birth
                OR t1.passport_valid_to = t2.passport_valid_to
            )
    """
    )


def update_clients_scd1():
    """
    - Insert the new records and update the existing records
    """

    # new records
    cursor.execute(
        """
        INSERT OR IGNORE INTO DWH_DIM_CLIENTS (
            client_id,
            last_name,
            first_name,
            patronymic,
            date_of_birth,
            passport_num,
            passport_valid_to,
            phone,
            create_dt,
            update_dt
        ) SELECT
            client_id,
            last_name,
            first_name,
            patronymic,
            date_of_birth,
            passport_num,
            passport_valid_to,
            phone,
            create_dt,
            update_dt
        FROM
            STG_NOT_MATCHED_ROWS_CLIENTS
    """
    )

    # overwrite existing data
    cursor.execute(
        """
        UPDATE
            DWH_DIM_CLIENTS
        SET
            last_name = (
                SELECT
                    last_name
                FROM
                    STG_MATCHED_ROWS_CLIENTS
                WHERE
                    client_id = DWH_DIM_CLIENTS.client_id
            ),
            first_name = (
                SELECT
                    first_name
                FROM
                    STG_MATCHED_ROWS_CLIENTS
                WHERE
                    client_id = DWH_DIM_CLIENTS.client_id
            ),
            patronymic = (
                SELECT
                    patronymic
                FROM
                    STG_MATCHED_ROWS_CLIENTS
                WHERE
                    client_id = DWH_DIM_CLIENTS.client_id
            ),
            date_of_birth = (
                SELECT
                    date_of_birth
                FROM
                    STG_MATCHED_ROWS_CLIENTS
                WHERE
                    client_id = DWH_DIM_CLIENTS.client_id
            ),
            passport_num = (
                SELECT
                    passport_num
                FROM
                    STG_MATCHED_ROWS_CLIENTS
                WHERE
                    client_id = DWH_DIM_CLIENTS.client_id
            ),
            passport_valid_to = (
                SELECT
                    passport_valid_to
                FROM
                    STG_MATCHED_ROWS_CLIENTS
                WHERE
                    client_id = DWH_DIM_CLIENTS.client_id
            ),
            phone = (
                SELECT
                    phone
                FROM
                    STG_MATCHED_ROWS_CLIENTS
                WHERE
                    client_id = DWH_DIM_CLIENTS.client_id
            ),
            create_dt = (
                SELECT
                    create_dt
                FROM
                    STG_MATCHED_ROWS_CLIENTS
                WHERE
                    client_id = DWH_DIM_CLIENTS.client_id
            ),
            update_dt = (
                SELECT
                    update_dt
                FROM
                    STG_MATCHED_ROWS_CLIENTS
                WHERE
                    client_id = DWH_DIM_CLIENTS.client_id
            )
        WHERE
            EXISTS (
                SELECT
                    last_name
                FROM
                    STG_MATCHED_ROWS_CLIENTS
                WHERE
                    client_id = DWH_DIM_CLIENTS.client_id
            )
            OR EXISTS (
                SELECT
                    first_name
                FROM
                    STG_MATCHED_ROWS_CLIENTS
                WHERE
                    client_id = DWH_DIM_CLIENTS.client_id
            )
            OR EXISTS (
                SELECT 
                    patronymic
                FROM 
                    STG_MATCHED_ROWS_CLIENTS
                WHERE 
                    client_id = DWH_DIM_CLIENTS.client_id
            )
            OR EXISTS (
                SELECT
                    date_of_birth
                FROM
                    STG_MATCHED_ROWS_CLIENTS
                WHERE
                    client_id = DWH_DIM_CLIENTS.client_id
            )
            OR EXISTS (
                SELECT
                    passport_num
                FROM
                    STG_MATCHED_ROWS_CLIENTS
                WHERE
                    client_id = DWH_DIM_CLIENTS.client_id
            )
            OR EXISTS (
                SELECT
                    passport_valid_to
                FROM
                    STG_MATCHED_ROWS_CLIENTS
                WHERE
                    client_id = DWH_DIM_CLIENTS.client_id
            )
            OR EXISTS (
                SELECT
                    phone
                FROM
                    STG_MATCHED_ROWS_CLIENTS
                WHERE
                    client_id = DWH_DIM_CLIENTS.client_id
            )
            OR EXISTS (
                SELECT
                    create_dt
                FROM
                    STG_MATCHED_ROWS_CLIENTS
                WHERE
                    client_id = DWH_DIM_CLIENTS.client_id
            )
            OR EXISTS (
                SELECT
                    update_dt
                FROM
                    STG_MATCHED_ROWS_CLIENTS
                WHERE
                    client_id = DWH_DIM_CLIENTS.client_id
            )
    """
    )

    # saves all the modifications
    conn.commit()

# LOADING SCD2 table: DWH_DIM_CLIENTS_HIST


def init_view_clients():
    """
    - Create staging view: STG_VIEW_CLIENTS
    """

    cursor.execute(
        """
        CREATE VIEW IF NOT EXISTS STG_VIEW_CLIENTS
        AS
        SELECT
            client_id,
            last_name,
            first_name,
            patronymic,
            date_of_birth,
            passport_num,
            passport_valid_to,
            phone
        FROM
            DWH_DIM_CLIENTS_HIST
        WHERE
            CURRENT_TIMESTAMP BETWEEN effective_from AND effective_to
            AND deleted_flg = 0
    """
    )


def create_new_rows_clients():
    """
    - Create staging table: STG_NEW_ROWS_CLIENTS
    - Select the new records
    """

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS STG_NEW_ROWS_CLIENTS
        AS
        SELECT
            t1.client_id,
            t1.last_name,
            t1.first_name,
            t1.patronymic,
            t1.date_of_birth,
            t1.passport_num,
            t1.passport_valid_to,
            t1.phone
        FROM
            DWH_DIM_CLIENTS t1
            LEFT JOIN STG_VIEW_CLIENTS t2 ON t1.client_id = t2.client_id
        WHERE
            t2.client_id IS NULL
    """
    )


def create_deleted_rows_clients():
    """
    - Create staging table: STG_DELETED_ROWS_CLIENTS
    - Soft delete the records
    """

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS STG_DELETED_ROWS_CLIENTS
        AS
        SELECT
            t1.client_id,
            t1.last_name,
            t1.first_name,
            t1.patronymic,
            t1.date_of_birth,
            t1.passport_num,
            t1.passport_valid_to,
            t1.phone
        FROM
            STG_VIEW_CLIENTS t1
            LEFT JOIN DWH_DIM_CLIENTS t2 ON t1.client_id = t2.client_id
        WHERE
            t2.client_id IS NULL
    """
    )


def create_changed_rows_clients():
    """
    - Create staging table: STG_CHANGED_ROWS_CLIENTS
    - Select changed rows
    """

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS STG_CHANGED_ROWS_CLIENTS
        AS
        SELECT
            t1.client_id,
            t1.last_name,
            t1.first_name,
            t1.patronymic,
            t1.date_of_birth,
            t1.passport_num,
            t1.passport_valid_to,
            t1.phone
        FROM
            DWH_DIM_CLIENTS t1
            INNER JOIN STG_VIEW_CLIENTS t2 ON t1.client_id = t2.client_id
            AND (
                t1.phone <> t2.phone
                OR t1.last_name <> t2.last_name
                OR t1.first_name <> t2.first_name
                OR t1.patronymic <> t2.patronymic
                OR t1.passport_num <> t2.passport_num
                OR t1.date_of_birth <> t2.date_of_birth
                OR t1.passport_valid_to <> t2.passport_valid_to
            )
    """
    )


def update_clients_scd2():
    """
    - Add new row
    """

    # new records
    cursor.execute(
        """
        INSERT INTO DWH_DIM_CLIENTS_HIST (
            client_id,
            last_name,
            first_name,
            patronymic,
            date_of_birth,
            passport_num,
            passport_valid_to,
            phone
        ) SELECT
            client_id,
            last_name,
            first_name,
            patronymic,
            date_of_birth,
            passport_num,
            passport_valid_to,
            phone
        FROM
            STG_NEW_ROWS_CLIENTS
    """
    )

    # modified records
    cursor.execute(
        """
        UPDATE
            DWH_DIM_CLIENTS_HIST
        SET
            effective_to = DATETIME('now', '-1 second')
        WHERE
            client_id IN (
                SELECT
                    client_id
                FROM
                    STG_CHANGED_ROWS_CLIENTS
            )
            AND effective_to = DATETIME('2999-12-31 23:59:59')
    """
    )

    cursor.execute(
        """
        INSERT INTO DWH_DIM_CLIENTS_HIST (
            client_id,
            last_name,
            first_name,
            patronymic,
            date_of_birth,
            passport_num,
            passport_valid_to,
            phone
        ) SELECT
            client_id,
            last_name,
            first_name,
            patronymic,
            date_of_birth,
            passport_num,
            passport_valid_to,
            phone
        FROM
            STG_CHANGED_ROWS_CLIENTS
    """
    )

    # soft deleted records
    cursor.execute(
        """
        UPDATE
            DWH_DIM_CLIENTS_HIST
        SET
            effective_to = DATETIME('now', '-1 second')
        WHERE
            client_id IN (
                SELECT
                    client_id
                FROM
                    STG_DELETED_ROWS_CLIENTS
            )
            AND effective_to = DATETIME('2999-12-31 23:59:59')
    """
    )

    cursor.execute(
        """
        INSERT INTO DWH_DIM_CLIENTS_HIST (
            client_id,
            last_name,
            first_name,
            patronymic,
            date_of_birth,
            passport_num,
            passport_valid_to,
            phone,
            deleted_flg
        ) SELECT
            client_id,
            last_name,
            first_name,
            patronymic,
            date_of_birth,
            passport_num,
            passport_valid_to,
            phone,
            1
        FROM
            STG_DELETED_ROWS_CLIENTS
    """
    )

    # saves all the modifications
    conn.commit()

# TERMINALS

# LOADING SCD2 table: DWH_DIM_TERMINALS_HIST


def init_view_terminals():
    """
    - Create staging view: STG_VIEW_TERMINALS
    """

    cursor.execute(
        """
        CREATE VIEW IF NOT EXISTS STG_VIEW_TERMINALS
        AS
        SELECT
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address
        FROM
            DWH_DIM_TERMINALS_HIST
        WHERE
            CURRENT_TIMESTAMP BETWEEN effective_from AND effective_to
            AND deleted_flg = 0
    """
    )


def create_new_rows_terminals():
    """
    - Create staging table: STG_NEW_ROWS_TERMINALS
    - Select the new records
    """

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS STG_NEW_ROWS_TERMINALS
        AS
        SELECT
            t1.terminal_id,
            t1.terminal_type,
            t1.terminal_city,
            t1.terminal_address
        FROM
            STG_TERMINALS t1
            LEFT JOIN STG_VIEW_TERMINALS t2 ON t1.terminal_id = t2.terminal_id
        WHERE
            t2.terminal_id IS NULL
    """
    )


def create_deleted_rows_terminals():
    """
    - Create staging table: STG_DELETED_ROWS_TERMINALS
    - Soft delete the records
    """

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS STG_DELETED_ROWS_TERMINALS
        AS
        SELECT
            t1.terminal_id,
            t1.terminal_type,
            t1.terminal_city,
            t1.terminal_address
        FROM
            STG_VIEW_TERMINALS t1
            LEFT JOIN STG_TERMINALS t2 ON t1.terminal_id = t2.terminal_id
        WHERE
            t2.terminal_id IS NULL
    """
    )


def create_changed_rows_terminals():
    """
    - Create staging table: STG_CHANGED_ROWS_TERMINALS
    - Select changed rows
    """

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS STG_CHANGED_ROWS_TERMINALS
        AS
        SELECT
            t1.terminal_id,
            t1.terminal_type,
            t1.terminal_city,
            t1.terminal_address
        FROM STG_TERMINALS t1
            INNER JOIN STG_VIEW_TERMINALS t2 ON t1.terminal_id = t2.terminal_id
            AND (
                t1.terminal_type <> t2.terminal_type
                OR t1.terminal_city <> t2.terminal_city
                OR t1.terminal_address <> t2.terminal_address
            )
    """
    )


def update_hist_terminals():
    """
    - Add new row
    """

    # new records
    cursor.execute(
        """
        INSERT INTO DWH_DIM_TERMINALS_HIST (
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address
        ) SELECT
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address
        FROM
            STG_NEW_ROWS_TERMINALS
    """
    )

    # modified records
    cursor.execute(
        """
        UPDATE
            DWH_DIM_TERMINALS_HIST
        SET
            effective_to = DATETIME('now', '-1 second')
        WHERE
            terminal_id IN (
                SELECT
                    terminal_id
                FROM
                    STG_CHANGED_ROWS_TERMINALS
            )
            AND effective_to = DATETIME('2999-12-31 23:59:59')
    """
    )

    cursor.execute(
        """
        INSERT INTO DWH_DIM_TERMINALS_HIST (
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address
        ) SELECT
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address
        FROM
            STG_CHANGED_ROWS_TERMINALS
    """
    )

    # soft deleted records
    cursor.execute(
        """
        UPDATE
            DWH_DIM_TERMINALS_HIST
        SET
            effective_to = DATETIME('now', '-1 second')
        WHERE
            terminal_id IN (
                SELECT
                    terminal_id
                FROM
                    STG_DELETED_ROWS_TERMINALS
            )
            AND effective_to = DATETIME('2999-12-31 23:59:59')
    """
    )

    cursor.execute(
        """
        INSERT INTO DWH_DIM_TERMINALS_HIST (
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            deleted_flg
        ) SELECT
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            1
        FROM
            STG_DELETED_ROWS_TERMINALS
    """
    )

    # saves all the modifications
    conn.commit()

# LOADING FACT table: DWH_FACT_TRANSACTIONS


def load_fact_transactions():
    """
    - Loading DWH_FACT_TRANSACTIONS
    """

    cursor.execute(
        """
        INSERT INTO DWH_FACT_TRANSACTIONS (
            trans_id,
            trans_date,
            card_num,
            oper_type,
            amt,
            oper_result,
            terminal
        ) SELECT
            t1.trans_id,
            t1.trans_date,
            t1.card_num,
            t1.oper_type,
            t1.amt,
            t1.oper_result,
            t1.terminal
        FROM
            STG_TRANSACTIONS t1
        WHERE
            NOT EXISTS (
                SELECT
                    trans_id
                    trans_date,
                    card_num,
                    oper_type,
                    amt,
                    oper_result,
                    terminal
                FROM
                    DWH_FACT_TRANSACTIONS t2
                WHERE
                    t2.amt = t1.amt
                    AND t2.trans_id = t1.trans_id
                    AND t2.card_num = t1.card_num
                    AND t2.terminal = t1.terminal
                    AND t2.oper_type = t1.oper_type
                    AND t2.trans_date = t1.trans_date
                    AND t2.oper_result = t1.oper_result
            )
    """
    )

    # saves all the modifications
    conn.commit()

# LOADING FACT table: DWH_FACT_PASSPORT_BLACKLIST


def load_fact_passport_blacklist():
    """
    - Loading DWH_FACT_PASSPORT_BLACKLIST
    """

    cursor.execute(
        """
        INSERT INTO DWH_FACT_PASSPORT_BLACKLIST (
            passport_num,
            entry_dt
        ) SELECT
            t1.passport_num,
            t1.entry_dt
        FROM
            STG_PASSPORT_BLACKLIST t1
        WHERE
            NOT EXISTS (
                SELECT
                    passport_num,
                    entry_dt
                FROM
                    DWH_FACT_PASSPORT_BLACKLIST t2
                WHERE
                    t2.passport_num = t1.passport_num
                    AND t2.entry_dt = t1.entry_dt
        )
    """
    )

    # saves all the modifications
    conn.commit()


def load_dim_fact_tables():
    """
    - Loading all tables: SCD1, SCD2 and FACT
    """

    # DWH_DIM_CARDS
    init_stg_cards()
    create_not_matched_rows_cards()
    create_matched_rows_cards()
    update_cards_scd1()

    # DWH_DIM_CARDS_HIST
    init_view_cards()
    create_new_rows_cards()
    create_deleted_rows_cards()
    create_changed_rows_cards()
    update_cards_scd2()

    # DWH_DIM_ACCOUNTS
    init_stg_accounts()
    create_not_matched_rows_accounts()
    create_matched_rows_accounts()
    update_accounts_scd1()

    # DWH_DIM_ACCOUNTS_HIST
    init_view_accounts()
    create_new_rows_accounts()
    create_deleted_rows_accounts()
    create_changed_rows_accounts()
    update_accounts_scd2()

    # DWH_DIM_CLIENTS
    init_stg_clients()
    create_not_matched_rows_clients()
    create_matched_rows_clients()
    update_clients_scd1()

    # DWH_DIM_CLIENTS_HIST
    init_view_clients()
    create_new_rows_clients()
    create_deleted_rows_clients()
    create_changed_rows_clients()
    update_clients_scd2()

    # DWH_DIM_TERMINALS_HIST
    init_view_terminals()
    create_new_rows_terminals()
    create_deleted_rows_terminals()
    create_changed_rows_terminals()
    update_hist_terminals()

    # DWH_FACT_TRANSACTIONS
    load_fact_transactions()

    # DWH_FACT_PASSPORT_BLACKLIST
    load_fact_passport_blacklist()