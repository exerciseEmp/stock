"""
聚宽数据
"""
from datetime import time
import configparser

import pandas
import timestamp as timestamp
from jqdatasdk import *
from pymysql import Timestamp
from sqlalchemy import create_engine
from sqlalchemy.future import engine

from src.utils.DBUtils import *

query_sql = "SELECT * FROM stock_basic WHERE `code_str` like %s "


class stock_info:
    pass


"""
是否ST
"""
"""
每天更新数据
"""
"""
日K级别数据
"""
# 必须登录
cf = configparser.ConfigParser()
cf.read("../../config/config.properties", encoding='UTF-8')
auth(cf.get("config", "username"), cf.get("config", "password"))
"""
    获取所有股票数据
    display_name # 中文名称
    name # 缩写简称
    start_date # 上市日期
    end_date # 退市日期，如果没有退市则为2200-01-01
    type # 类型，stock(股票)
"""


def save_stock_k(code_str: str):
    """
        保存k线到数据库
        默认获取所有的k线图
        会自动append最新数据
    :param code_str: 股票代码
    :return:
    """

    connection = PooledDB.connection()
    cursor = connection.cursor()
    cursor.execute(query_sql, code_str)
    data = cursor.fetchone()
    stock_k: pandas.DataFrame = get_price(code_str, start_date=data[3], end_date=data[4],
                                          frequency='daily',
                                          fields=None, skip_paused=False,
                                          fq='pre', count=None, panel=True, fill_paused=True)
    engine = create_engine(f'mysql+pymysql://root:root@127.0.0.1:3306/stock')
    stock_k.to_sql(name="stock_k_day", con=engine)
    connection.commit()
    cursor.close()
    connection.close()
    pass


save_stock_k("000002.XSHE")


def update_and_insert_data():
    """
    每日查看是否有数据更新
    """
    all_data: pandas.DataFrame = get_all_securities(types=['stock'], date=None)
    connection = PooledDB.connection()
    cursor = connection.cursor()
    update_sql = "UPDATE stock_basic SET `display_name` = %s, `name` = %s ,`start_date` = %s ,`end_date`= %s,`type` = %s WHERE `code_str` =  %s "
    insert_sql = "INSERT INTO stock_basic(`code_str`,`display_name`,`name`,`start_date`,`end_date`,`type`) VALUES(%s, %s ,  %s , %s , %s ,%s) "
    code_strs = list(all_data.index)
    i = 0
    insert_int = 0
    update_int = 0
    for code_str in code_strs:
        data = all_data.loc[code_str]
        query_data = cursor.execute(query_sql, code_str)
        data_list = list(data.values)
        data_list.insert(0, code_str)
        # 没有就插入
        if query_data == 0:
            cursor.execute(insert_sql, tuple(data_list))
            print("插入数据", data_list)
            insert_int += 1
            pass
        else:
            mysql_data = cursor.fetchone()
            mysql_data = list(mysql_data)
            if mysql_data != data_list:
                # 数据不一致就更新
                remove = data_list.pop(0)
                data_list.insert(len(data_list), remove)
                cursor.execute(update_sql, data_list)
                print("更新数据", data_list)
                update_int += 1
                pass
        i += 1
        if i % 50 == 0:
            connection.commit()
            pass
    pass
    connection.commit()
    cursor.close()
    connection.close()
    print("插入数据一共", insert_int)
    print("更新数据一共", update_int)
    pass


# 更新股票基础数据
# update_and_insert_data()

stock_k = get_price("000001.XSHE", start_date='1991-04-03 00:00:00', end_date='2022-11-08 00:00:00', frequency='daily',
                    fields=None, skip_paused=False,
                    fq='pre', count=None, panel=True, fill_paused=True)
