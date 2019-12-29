# テーブル情報

- DBMS
    - MariaDB
- データベース名
    - rwsoa_japan_stock
- テーブル名
    - 決算・財務・業績時系列データ
- データ提供元
    - KABU+：決算・財務・業績データ
- データソース
    - japan-all-stock-financial-results_{YYYYMMDD}.csv

# 項目属性

| No | 論理名 | 物理名 | データ型 | PK | NOT NULL | Default | 備考 |
|---:|:---|:---|:---|:---:|:---:|:---|:---|
|1 |security_code |証券コード |int(11) |○ |○ |0 | |
|2 |target_month |日付 |varchar(6) |○ |○ | |YYYYMM |
|3 |company_name |会社名 |varchar(50) | | | | |
|4 |settlement_period |決算期 |varchar(7)) | | | | |
|5 |main_earnings_announcement_day |決算発表日（本決算） |date | | | | |
|6 |sales_amount |売上高（百万円） |double | | | | |
|7 |operating_income |営業利益（百万円） |double | | | | |
|8 |ordinary_profit |経常利益（百万円） |double | | | | |
|9 |net_income |当期利益（百万円） |double | | | | |
|10 |total_asset |総資産（百万円） |double | | | | |
|11 |net_worth |自己資本（百万円） |double | | | | |
|12 |capital |資本金（百万円） |double | | | | |
|13 |interest_bearing_debt |有利子負債（百万円） |int(11) | | | | |
|14 |capital_adequacy_ratio |自己資本比率 |int(11) | | | | |
|15 |ROE |自己資本利益率 |int(11) | | | | |
|16 |ROA |総資産利益率 |double | | | | |
|17 |issued_shares |総資産利益率 |double | | | | |