# テーブル情報

- DBMS
    - MariaDB
- データベース名
    - rwsoa_japan_stock
- テーブル名
    - 株価一覧表時系列データ

# 項目属性

| No | 論理名 | 物理名 | データ型 | PK | NOT NULL | Default | 備考 |
|---:|:---|:---|:---|:---:|:---:|:---|:---|
|1 |security_code |証券コード |int(11) |○ |○ |0 | |
|2 |dt |日付 |date |○ |○ |0000-00-00 |YYYY-MM-DD |
|3 |company_name |会社名 |varchar(50) | | | | |
|4 |stock_exchange_code |取引所コード |int(11) | | | | |
|5 |industry_type |業種 |int(11) | | | | |
|6 |opening_price |始値 |double | | | | |
|7 |closing_price |終値 |double | | | | |
|8 |high_price |高値 |double | | | | |
|9 |low_price |安値 |double | | | | |
|10 |day_before_ratio |前日比 |double | | | | |
|11 |day_before_ratio_percentage |前日比(%) |double | | | | |
|12 |last_day_closing_price |前日終値 |double | | | | |
|13 |volume |出来高 |int(11) | | | | |
|14 |trading_value |売買代金(千円) |int(11) | | | | |
|15 |market_capitalization |時価総額(百万円) |int(11) | | | | |
|16 |price_range_lower_limit |値幅下限 |double | | | | |
|17 |price_range_upper_limit |値幅上限 |double | | | | |