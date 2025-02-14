CREATE TABLE IF NOT EXISTS elec_futures_market (
    trading_date DATE,
    delivery_start DATE,
    delivery_end DATE,
    source VARCHAR(20),
    country CHAR(2), --ISO 3166-1 alpha-2
    peak BOOLEAN,
    tenor VARCHAR(20),
    settlement_price NUMERIC(10, 2),
    unit VARCHAR(10),
    PRIMARY KEY (trading_date, delivery_start, delivery_end, source, country, peak)
);

CREATE TABLE IF NOT EXISTS elec_day_ahead_market (
    delivery_start TIMESTAMP,
    delivery_end TIMESTAMP,
    source VARCHAR(20),
    country CHAR(2), --ISO 3166-1 alpha-2
    tenor VARCHAR(20),
    price NUMERIC(10, 2),
    unit VARCHAR(10),
    PRIMARY KEY (delivery_start, delivery_end, source, country)
);

CREATE TABLE IF NOT EXISTS gas_day_ahead_market (
    trading_date DATE,
    delivery_start DATE,
    delivery_end DATE,
    source VARCHAR(20),
    country CHAR(2), --ISO 3166-1 alpha-2
    tenor VARCHAR(20),
    last_price NUMERIC(10, 2),
    unit VARCHAR(10),
    PRIMARY KEY (trading_date, delivery_start, delivery_end, source, country)
);

CREATE TABLE IF NOT EXISTS consumption (
    cons_start TIMESTAMP,
    cons_end TIMESTAMP,
    source VARCHAR(20),
    country CHAR(2), --ISO 3166-1 alpha-2
    tenor VARCHAR(20),
    curve_type VARCHAR(30),
    quantity NUMERIC(10, 2),
    unit VARCHAR(5),
    PRIMARY KEY (cons_start, cons_end, source, country, curve_type)
);

CREATE TABLE IF NOT EXISTS production_per_type (
    prod_start TIMESTAMP,
    prod_end TIMESTAMP,
    source VARCHAR(20),
    country CHAR(2), --ISO 3166-1 alpha-2
    tenor VARCHAR(20),
    production_type VARCHAR(50),
    quantity NUMERIC(10, 2),
    unit VARCHAR(5),
    PRIMARY KEY (prod_start, prod_end, source, country, production_type)
);

CREATE TABLE IF NOT EXISTS installed_capacities (
    capa_start TIMESTAMP,
    capa_end TIMESTAMP,
    source VARCHAR(20),
    country CHAR(2), --ISO 3166-1 alpha-2
    tenor VARCHAR(20),
    capacities_type VARCHAR(50),
    quantity NUMERIC(10, 2),
    unit VARCHAR(5),
    PRIMARY KEY (capa_start, capa_end, source, country, capacities_type)
);

CREATE TABLE IF NOT EXISTS temperature (
    temp_start TIMESTAMP,
    temp_end TIMESTAMP,
    source VARCHAR(20),
    country CHAR(2), --ISO 3166-1 alpha-2
    tenor VARCHAR(20),
    tn NUMERIC(10, 2),
    tr NUMERIC(10, 2),
    trs NUMERIC(10, 2),
    delta_t NUMERIC(10, 2),
    delta_ts NUMERIC(10, 2),
    unit VARCHAR(5),
    PRIMARY KEY (temp_start, temp_end, source, country)
);

CREATE TABLE IF NOT EXISTS generation_forecast (
    prod_start TIMESTAMP,
    prod_end TIMESTAMP,
    source VARCHAR(20),
    country CHAR(2), --ISO 3166-1 alpha-2
    forecast_type VARCHAR(20),
    production_type VARCHAR(50),
    tenor VARCHAR(20),
    quantity NUMERIC(10, 2),
    unit VARCHAR(5),
    PRIMARY KEY (prod_start, prod_end, source, country, forecast_type, production_type)
);

CREATE TABLE IF NOT EXISTS imbalance (
    imb_start TIMESTAMP,
    imb_end TIMESTAMP,
    source VARCHAR(20),
    country CHAR(2), --ISO 3166-1 alpha-2
    tenor VARCHAR(20),
    system_trend VARCHAR(30),
    imbalance NUMERIC(10, 2),
    unit VARCHAR(5),
    positive_imbalance_settlement_price NUMERIC(10, 2),
    negative_imbalance_settlement_price NUMERIC(10, 2),
    currency CHAR(3), --ISO 4217
    PRIMARY KEY (imb_start, imb_end, source, country)
);