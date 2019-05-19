CREATE TABLE rwsoa_japan_stock.correlation_coefficient_with_nikkei(
    security_code INTEGER,
    company_name VARCHAR(50),
    target_month VARCHAR(6),
    corr_coef_day_before_ratio_percentage DOUBLE,
    PRIMARY KEY (security_code, target_month)
);