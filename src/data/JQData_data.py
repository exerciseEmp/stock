"""
聚宽数据
"""
from datetime import time
import configparser

import pandas as pd
import timestamp as timestamp
from jqdatasdk import *
from pymysql import Timestamp

from src.utils.DBUtils import *

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


def format_data(data_list):
    for data in data_list:
        if isinstance(data, Timestamp):
            data = str(data)


def update_and_insert_data():
    """
    每日查看是否有数据更新
    """
    all_data = get_all_securities(types=['stock'], date=None)
    connection = PooledDB.connection()
    cursor = connection.cursor()
    query_sql = "SELECT * FROM stock_basic WHERE `code_str` like %s "
    update_sql = "UPDATE stock_basic SET `display_name` = %s, `name` = %s ,`start_date` = %s ,`end_date`= %s,`type` = %s WHERE `code_str` =  %s "
    insert_sql = "INSERT INTO stock_basic(`code_str`,`display_name`,`name`,`start_date`,`end_date`,`type`) VALUES(%s, %s ,  %s , %s , %s ,%s) "
    code_strs = list(all_data.index)
    for code_str in code_strs:
        data = all_data.loc[code_str]
        query_data = cursor.execute(query_sql, code_str)
        data_list = list(data.values)
        data_list.insert(0, code_str)
        format_data(data_list)
        i = 0
        # 没有就插入
        if query_data == 0:
            cursor.execute(insert_sql, tuple(data_list))
            pass
        else:
            mysql_data = cursor.fetchone()
            mysql_data = list(mysql_data)
            format_data(mysql_data)
            if mysql_data != data_list:
                # 数据不一致就更新
                remove = data_list.pop(0)
                data_list.insert(len(data_list), remove)
                cursor.execute(update_sql, data_list)
                pass
        i += 1
        if i % 50 == 0:
            connection.commit()
    connection.commit()
    cursor.close()
    connection.close()
    pass


update_and_insert_data()
