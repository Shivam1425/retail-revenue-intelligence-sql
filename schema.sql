/* =========================================================
SCHEMA FILE: Retail Demand and Revenue Intelligence
AUTHOR: Shivam Kumar
DIALECT: MySQL 8.0
PURPOSE: MySQL 8 schema and indexing strategy for an analysis-first retail model.
========================================================= */

CREATE DATABASE IF NOT EXISTS retail_intelligence;
USE retail_intelligence;

DROP TABLE IF EXISTS train;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS stores;
DROP TABLE IF EXISTS holidays_events;
DROP TABLE IF EXISTS oil;

CREATE TABLE stores (
    store_nbr SMALLINT PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(100) NOT NULL,
    type CHAR(1) NOT NULL,
    cluster TINYINT NOT NULL,
    INDEX idx_stores_state_city (state, city),
    INDEX idx_stores_cluster (cluster)
) ENGINE = InnoDB;

CREATE TABLE train (
    id BIGINT PRIMARY KEY,
    date DATE NOT NULL,
    store_nbr SMALLINT NOT NULL,
    family VARCHAR(100) NOT NULL,
    sales DECIMAL(14, 4) NOT NULL DEFAULT 0,
    onpromotion INT NOT NULL DEFAULT 0,
    CONSTRAINT uq_train_business_grain UNIQUE (date, store_nbr, family),
    CONSTRAINT fk_train_store FOREIGN KEY (store_nbr) REFERENCES stores(store_nbr),
    INDEX idx_train_date_store_family (date, store_nbr, family),
    INDEX idx_train_family_date_store (family, date, store_nbr),
    INDEX idx_train_store_date (store_nbr, date)
) ENGINE = InnoDB;

CREATE TABLE transactions (
    date DATE NOT NULL,
    store_nbr SMALLINT NOT NULL,
    transactions INT NOT NULL,
    PRIMARY KEY (date, store_nbr),
    CONSTRAINT fk_transactions_store FOREIGN KEY (store_nbr) REFERENCES stores(store_nbr),
    INDEX idx_transactions_store_date (store_nbr, date)
) ENGINE = InnoDB;

CREATE TABLE holidays_events (
    date DATE NOT NULL,
    type VARCHAR(30) NOT NULL,
    locale VARCHAR(30) NOT NULL,
    locale_name VARCHAR(100) NOT NULL,
    description VARCHAR(150) NOT NULL,
    transferred VARCHAR(10) NOT NULL,
    INDEX idx_holidays_date_type (date, type),
    INDEX idx_holidays_locale_scope (locale, locale_name, date)
) ENGINE = InnoDB;

CREATE TABLE oil (
    date DATE PRIMARY KEY,
    dcoilwtico DECIMAL(12, 4) NULL,
    INDEX idx_oil_date_price (date, dcoilwtico)
) ENGINE = InnoDB;

/* =========================================================
DESIGN NOTES

- `train` keeps both a surrogate primary key and a business-grain unique key.
- Indexes are chosen for the actual analytical workload in `analysis.sql`.
- `stores` supports holiday attribution through city/state lookups.
- `transactions` stays at the store-day grain for basket-quality analysis.
- `holidays_events` is indexed for National / Regional / Local applicability joins.
- `oil` remains sparse by design and is left nullable for macro analysis.

========================================================= */
