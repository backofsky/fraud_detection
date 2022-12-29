-- CREATE TABLES

-- create FACT tables

-- create table DWH_FACT_TRANSACTIONS 
CREATE TABLE IF NOT EXISTS DWH_FACT_TRANSACTIONS (
    trans_id VARCHAR(128),
    trans_date DATE,
    card_num VARCHAR(128),
    oper_type VARCHAR(128),
    amt DECIMAL(10,2),
    oper_result VARCHAR(128),
    terminal VARCHAR(128)
);

-- create table DWH_FACT_PASSPORT_BLACKLIST 
CREATE TABLE IF NOT EXISTS DWH_FACT_PASSPORT_BLACKLIST (
    passport_num VARCHAR(128),
    entry_dt DATE
);

-- create a SCD2 table for where data will be loaded

-- create DWH_DIM_CARDS_HIST
CREATE TABLE IF NOT EXISTS DWH_DIM_CARDS_HIST (
    card_num VARCHAR(128),
    account_num VARCHAR(128),
    deleted_flg INTEGER DEFAULT 0,
    effective_from DATETIME DEFAULT (
        DATETIME('2001-01-01')
    ),
    effective_to DATETIME DEFAULT (
        DATETIME('2999-12-31 23:59:59')
    )
);

-- create DWH_DIM_ACCOUNTS_HIST
CREATE TABLE IF NOT EXISTS DWH_DIM_ACCOUNTS_HIST (
    account_num VARCHAR(128) PRIMARY KEY,
    valid_to DATE,
    client VARCHAR(128),
    deleted_flg INTEGER DEFAULT 0,
    effective_from DATETIME DEFAULT (
        DATETIME('1900-01-01')
    ),
    effective_to DATETIME DEFAULT (
        DATETIME('2999-12-31 23:59:59')
    )
);

-- create DWH_DIM_CLIENTS_HIST
CREATE TABLE IF NOT EXISTS DWH_DIM_CLIENTS_HIST (
    client_id VARCHAR(128),
    last_name VARCHAR(128),
    first_name VARCHAR(128),
    patronymic VARCHAR(128),
    date_of_birth DATE,
    passport_num VARCHAR(128),
    passport_valid_to DATE,
    phone VARCHAR(128),
    deleted_flg INTEGER DEFAULT 0,
    effective_from DATETIME DEFAULT (
        DATETIME('1900-01-01')
    ),
    effective_to DATETIME DEFAULT (
        DATETIME('2999-12-31 23:59:59')
    )
);

-- create DWH_DIM_TERMINALS_HIST
CREATE TABLE IF NOT EXISTS DWH_DIM_TERMINALS_HIST (
    terminal_id VARCHAR(128),
    terminal_type VARCHAR(128),
    terminal_city VARCHAR(128),
    terminal_address VARCHAR(128),
    deleted_flg INTEGER DEFAULT 0,
    effective_from DATETIME DEFAULT CURRENT_TIMESTAMP,
    effective_to DATETIME DEFAULT (
        DATETIME('2999-12-31 23:59:59')
    )
);

-- Data Mart

-- create report table: REP_FRAUD
CREATE TABLE IF NOT EXISTS REP_FRAUD (
    event_dt DATE,
    passport VARCHAR(128),
    fio VARCHAR(364),
    phone VARCHAR(128),
    event_type VARCHAR(128),
    report_dt DATE DEFAULT CURRENT_TIMESTAMP
);

-- create META_TABLE
CREATE TABLE IF NOT EXISTS META_TABLE AS
    SELECT
        tbl_name,
        DATETIME("2001-01-01") AS last_update
    FROM
        SQLITE_MASTER
    WHERE
        type != "index"
        AND tbl_name NOT LIKE "sqlite%"
        AND tbl_name NOT LIKE "META%";

-- Create a STG tables

-- create STG_CARDS
CREATE TABLE IF NOT EXISTS STG_CARDS (
    card_num VARCHAR(128),
    account_num VARCHAR(128),
    create_dt DATE,
    update_dt DATE
);

-- create STG_ACCOUNTS 
CREATE TABLE IF NOT EXISTS STG_ACCOUNTS (
    account_num VARCHAR(128),
    valid_to DATE,
    client VARCHAR(128),
    create_dt DATE,
    update_dt DATE
);

-- create STG_CLIENTS
CREATE TABLE IF NOT EXISTS STG_CLIENTS (
    client_id VARCHAR(128),
    last_name VARCHAR(128),
    first_name VARCHAR(128),
    patronymic VARCHAR(128),
    date_of_birth DATE,
    passport_num VARCHAR(128),
    passport_valid_to DATE,
    phone VARCHAR(128),
    create_dt DATE,
    update_dt DATE
);

-- create STG_REP_FRAUD
CREATE TABLE IF NOT EXISTS STG_REP_FRAUD (
    event_dt DATE,
    passport VARCHAR(128),
    fio VARCHAR(364),
    phone VARCHAR(128),
    event_type VARCHAR(128),
    report_dt DATE DEFAULT CURRENT_TIMESTAMP
);

-- Create a SCD1 tables for where data will be loaded

-- create DWH_DIM_CARDS
CREATE TABLE IF NOT EXISTS DWH_DIM_CARDS (
    card_num VARCHAR(128),
    account_num VARCHAR(128),
    create_dt DATE,
    update_dt DATE
);

-- create DWH_DIM_ACCOUNTS 
CREATE TABLE IF NOT EXISTS DWH_DIM_ACCOUNTS (
    account_num VARCHAR(128),
    valid_to DATE,
    client VARCHAR(128),
    create_dt DATE,
    update_dt DATE
);

-- create DWH_DIM_CLIENTS
CREATE TABLE IF NOT EXISTS DWH_DIM_CLIENTS (
    client_id VARCHAR(128),
    last_name VARCHAR(128),
    first_name VARCHAR(128),
    patronymic VARCHAR(128),
    date_of_birth DATE,
    passport_num VARCHAR(128),
    passport_valid_to DATE,
    phone VARCHAR(128),
    create_dt DATE,
    update_dt DATE
);