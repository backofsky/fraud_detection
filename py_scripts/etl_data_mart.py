# Importing dependencies
import datetime
from main import conn, cursor

# DATA MART


def detection_passport_fraud(date: str):
    """
    - Searching passport fraud
    """

    # convert date to format '%Y-%m-%d'
    date = datetime.datetime.strptime(date, "%d%m%Y").date()

    cursor.execute(
        """
        INSERT INTO STG_REP_FRAUD (
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
        ) SELECT
            t4.trans_date AS event_dt,
            t1.passport_num AS passport,
            t1.last_name || ' ' || t1.first_name || ' ' || t1.patronymic AS fio,
            t1.phone,
            1 AS event_type,
            DATETIME('now') AS report_dt
        FROM
            DWH_DIM_CLIENTS t1
            INNER JOIN DWH_DIM_ACCOUNTS_HIST t2 ON t1.client_id = t2.client
            INNER JOIN DWH_DIM_CARDS_HIST t3 ON t2.account_num = t3.account_num
            INNER JOIN DWH_FACT_TRANSACTIONS t4 ON t3.card_num = t4.card_num
        WHERE
            ? >= t1.passport_valid_to
            OR t1.passport_num IN (
                SELECT
                    passport_num
                FROM
                    DWH_FACT_PASSPORT_BLACKLIST
            )
        """,
        [date]
    )

    # saves all the modifications
    conn.commit()


def detection_account_fraud(date: str):
    """
    - Searching account fraud
    """

    # convert date to format '%Y-%m-%d'
    date = datetime.datetime.strptime(date, "%d%m%Y").date()

    cursor.execute(
        """
        INSERT INTO STG_REP_FRAUD (
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
        ) SELECT
            t4.trans_date AS event_dt,
            t1.passport_num,
            t1.last_name || ' ' || t1.first_name || ' ' || t1.patronymic AS fio,
            t1.phone,
            2 AS event_type,
            DATETIME('now')
        FROM
            DWH_DIM_CLIENTS t1
            LEFT JOIN DWH_DIM_ACCOUNTS_HIST t2 ON t1.client_id = t2.client
            LEFT JOIN DWH_DIM_CARDS_HIST t3 ON t2.account_num = t3.account_num
            LEFT JOIN DWH_FACT_TRANSACTIONS t4 ON t3.card_num = t4.card_num
        WHERE
            ? >= t2.valid_to
    """,
        [date]
    )

    # saves all the modifications
    conn.commit()


def detection_different_city_fraud():
    """
    - Searching different city fraud
    """

    cursor.execute(
        """
        CREATE VIEW IF NOT EXISTS STG_DETECTION_FRAUD_DIFFERENT_CITY
        AS
        SELECT
            t1.card_num,
            t1.trans_date AS current_city_date,
            t2.terminal_city AS current_city,
            LEAD(t2.terminal_city) OVER(
                PARTITION BY t1.card_num
                ORDER BY t1.trans_date
            ) AS different_city,
            LEAD(t1.trans_date) OVER(
                PARTITION BY t1.card_num
                ORDER BY t1.trans_date
            ) AS different_city_date
        FROM
            DWH_FACT_TRANSACTIONS t1
            INNER JOIN DWH_DIM_TERMINALS_HIST t2 ON t1.terminal = t2.terminal_id
        WHERE
            t1.card_num IN (
                SELECT
                    t1.card_num
                FROM
                    DWH_FACT_TRANSACTIONS t1
                    INNER JOIN DWH_DIM_TERMINALS_HIST t2 ON t1.terminal = t2.terminal_id
                GROUP BY
                    t1.card_num
                HAVING
                    COUNT(DISTINCT t2.terminal_city) > 1
            )
        """
    )

    # 3600 seconds = 60 minutes
    cursor.execute(
        """
        INSERT INTO STG_REP_FRAUD (
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
        ) SELECT
            MIN(t1.different_city_date) AS event_dt,
            t4.passport_num AS passport,
            t4.last_name || ' ' || t4.first_name || ' ' || t4.patronymic AS fio,
            t4.phone,
            3 AS event_type,
            DATETIME('now')
        FROM
            STG_DETECTION_FRAUD_DIFFERENT_CITY t1
            INNER JOIN DWH_DIM_CARDS_HIST t2 ON t1.card_num = t2.card_num
            INNER JOIN DWH_DIM_ACCOUNTS_HIST t3 ON t2.account_num = t3.account_num
            INNER JOIN DWH_DIM_CLIENTS_HIST t4 ON t3.client = t4.client_id
        WHERE
            STRFTIME('%s', t1.current_city_date) - STRFTIME('%s', t1.different_city_date) <= 3600
        GROUP BY
            t1.card_num
        """
    )

    # saves all the modifications
    conn.commit()


def detection_sum_up_fraud():
    """
    - Searching sum up fraud
    """

    cursor.execute(
        """
        CREATE VIEW IF NOT EXISTS STG_DETECTION_FRAUD_SUM_UP
        AS
        SELECT
            card_num,
            trans_date,
            oper_type,
            oper_result,
            amt,
            LAG(oper_result, 1) OVER (
                PARTITION BY card_num
                ORDER BY trans_date
            ) AS previous_oper_result_1,
            LAG(amt, 1) OVER (
                PARTITION BY card_num
                ORDER BY trans_date
            ) AS previous_amt_1,
            LAG(oper_result, 2) OVER (
                PARTITION BY card_num
                ORDER BY trans_date
            ) AS previous_oper_result_2,
            LAG(amt, 2) OVER (
                PARTITION BY card_num
                ORDER BY trans_date
            ) AS previous_amt_2,
            LAG(oper_result, 3) OVER (
                PARTITION BY card_num
                ORDER BY trans_date
            ) AS previous_oper_result_3,
            LAG(amt, 3) OVER (
                PARTITION BY card_num
                ORDER BY trans_date
            ) AS previous_amt_3,
            LAG(trans_date, 3) OVER (
                PARTITION BY card_num
                ORDER BY trans_date
            ) AS previous_date_3
        FROM
            DWH_FACT_TRANSACTIONS
        """
    )

    # 1200 seconds = 20 minutes
    cursor.execute(
        """
        INSERT INTO STG_REP_FRAUD (
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
        ) SELECT
            t1.trans_date AS event_dt,
            t4.passport_num AS passport,
            t4.last_name || ' ' || t4.first_name || ' ' || t4.patronymic AS fio,
            t4.phone,
            4 AS event_type,
            DATETIME('now')
        FROM
            STG_DETECTION_FRAUD_SUM_UP t1
            LEFT JOIN DWH_DIM_CARDS_HIST t2 ON t1.card_num = t2.card_num
            LEFT JOIN DWH_DIM_ACCOUNTS_HIST t3 ON t2.account_num = t3.account_num
            LEFT JOIN DWH_DIM_CLIENTS_HIST t4 ON t3.client = t4.client_id
        WHERE
            t1.oper_result = 'SUCCESS'
            AND t1.previous_oper_result_1 = 'REJECT'
            AND t1.previous_oper_result_2 = 'REJECT'
            AND t1.previous_oper_result_3 = 'REJECT'
            AND t1.amt < t1.previous_amt_3 < t1.previous_amt_2 < t1.previous_amt_1
            AND STRFTIME('%s', t1.trans_date) - STRFTIME('%s', t1.previous_date_3) <= 1200
        """
    )

    # saves all the modifications
    conn.commit()


def not_allow_duplicate():
    """
    - Insert without duplicate record
    """

    cursor.execute(
        """
        INSERT INTO REP_FRAUD(
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
        ) SELECT
            t1.event_dt,
            t1.passport,
            t1.fio,
            t1.phone,
            t1.event_type,
            t1.report_dt
        FROM
            STG_REP_FRAUD t1
        WHERE
            NOT EXISTS (
                SELECT
                    event_dt
                    passport,
                    fio,
                    phone,
                    event_type
                FROM
                    REP_FRAUD t2
                WHERE
                    t2.event_dt = t1.event_dt
                    AND t2.passport = t1.passport
                    AND t2.fio = t1.fio
                    AND t2.phone = t1.phone
                    AND t2.event_type = t2.event_type
        )
    """
    )


def build_data_mart(date: str):
    """
    - Build a Data Mart
    Fraud detection:
    - 1) passport fraud
    - 2) account fraud
    - 3) different city fraud
    - 4) sum up fraud
    """

    # searching passport fraud
    detection_passport_fraud(date)

    # searching account fraud
    detection_account_fraud(date)

    # searching different city fraud
    detection_different_city_fraud()

    # searching sum up fraud
    detection_sum_up_fraud()

    # avoiding duplicates
    not_allow_duplicate()
