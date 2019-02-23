CREATE TABLE rwsoa_japan_stock.nikkei225_topix_stock_prices(
    security_code INTEGER,
    dt DATE,
    name VARCHAR(50),
    opening_price DOUBLE,
    closing_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    day_before_ratio DOUBLE,
    day_before_ratio_percentage DOUBLE,
    last_day_closing_price DOUBLE,
    PRIMARY KEY (security_code, dt)
);