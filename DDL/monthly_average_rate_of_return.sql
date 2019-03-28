CREATE TABLE rwsoa_japan_stock.monthly_average_rate_of_return(
    security_code INTEGER,
    company_name VARCHAR(50),
    target_month VARCHAR(6),
    start_closing_price DOUBLE,
    end_closing_price DOUBLE,
    monthly_average_rate_of_return DOUBLE,
    PRIMARY KEY (security_code, target_month)
);