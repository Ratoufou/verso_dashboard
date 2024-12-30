CREATE TABLE IF NOT EXISTS elec_futures_market (
    trading_date DATE,
    delivery_start DATE,
    delivery_end DATE,
    source VARCHAR(20),
    country CHAR(2), --ISO 3166-1 alpha-2
    peak BOOLEAN,
    tenor VARCHAR(20),
    settlement_price NUMERIC(10, 2),
    currency CHAR(3), --ISO 4217
    PRIMARY KEY (trading_date, delivery_start, delivery_end, source, country, peak)
);

CREATE TABLE IF NOT EXISTS elec_day_ahead_market (
    delivery_start TIMESTAMP,
    delivery_end TIMESTAMP,
    source VARCHAR(20),
    country CHAR(2), --ISO 3166-1 alpha-2
    tenor VARCHAR(20),
    price NUMERIC(10, 2),
    currency CHAR(3), --ISO 4217
    PRIMARY KEY (delivery_start, delivery_end, source, country)
);

CREATE TABLE IF NOT EXISTS gas_day_ahead_market (
    trading_date TIMESTAMP,
    delivery_start TIMESTAMP,
    delivery_end TIMESTAMP,
    source VARCHAR(20),
    country CHAR(2), --ISO 3166-1 alpha-2
    tenor VARCHAR(20),
    last_price NUMERIC(10, 2),
    currency CHAR(3), --ISO 4217
    PRIMARY KEY (trading_date, delivery_start, delivery_end, source, country)
);

