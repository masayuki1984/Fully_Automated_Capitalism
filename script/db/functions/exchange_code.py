#coding:utf-8

def get_stock_exchange_code(stock_exchange_name):
    if stock_exchange_name == '東証一部':
        return 1
    elif stock_exchange_name == '東証二部':
        return 2
    elif stock_exchange_name == 'JQS':
        return 3
    elif stock_exchange_name == 'JQG':
        return 4
    elif stock_exchange_name == '東証マザ':
        return 5
    elif stock_exchange_name == '名証一部':
        return 6
    elif stock_exchange_name == '名証二部':
        return 7
    elif stock_exchange_name == '名証セント':
        return 8
    elif stock_exchange_name == '札証':
        return 9
    elif stock_exchange_name == '札証アンビ':
        return 10
    elif stock_exchange_name == '福証':
        return 11
    elif stock_exchange_name == '福証QB':
        return 12
    elif stock_exchange_name == '東証':
        return 13
    else:
        return 'null'


def get_industry_type(industry_name):
    if industry_name == '水産・農林':
        return 1
    elif industry_name == '鉱業':
        return 2
    elif industry_name == '建設':
        return 3
    elif industry_name == '食料品':
        return 4
    elif industry_name == '繊維製品':
        return 5
    elif industry_name == 'パルプ・紙':
        return 6
    elif industry_name == '化学':
        return 7
    elif industry_name == '医薬品':
        return 8
    elif industry_name == '石油・石炭':
        return 9
    elif industry_name == 'ゴム製品':
        return 10
    elif industry_name == 'ガラス土石':
        return 11
    elif industry_name == '鉄鋼':
        return 12
    elif industry_name == '非鉄金属':
        return 13
    elif industry_name == '金属製品':
        return 14
    elif industry_name == '機械':
        return 15
    elif industry_name == '電気機器':
        return 16
    elif industry_name == '輸送用機器':
        return 17
    elif industry_name == '精密機器':
        return 18
    elif industry_name == 'その他製品':
        return 19
    elif industry_name == '電気・ガス':
        return 20
    elif industry_name == '陸運':
        return 21
    elif industry_name =='海運':
        return 22
    elif industry_name == '空運':
        return 23
    elif industry_name == '倉庫・運輸':
        return 24
    elif industry_name == '情報通信':
        return 25
    elif industry_name == '卸売':
        return 26
    elif industry_name == '小売':
        return 27
    elif industry_name == '銀行':
        return 28
    elif industry_name == '証券・先物':
        return 29
    elif industry_name == '保険':
        return 30
    elif industry_name == 'その他金融':
        return 31
    elif industry_name == '不動産':
        return 32
    elif industry_name == 'サービス':
        return 33
    if industry_name == 'ETF':
        return 34
    elif industry_name == 'ETN':
        return 35
    else:
        return 'null'