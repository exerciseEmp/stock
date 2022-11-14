"""
聚宽数据
Runs very slowly
because single thread and sql
"""
import math
import configparser
import traceback

import pandas
import timestamp as timestamp
from jqdatasdk import *

from src.utils.DBUtils import *


class stock_info:
    """
        获取所有股票数据
        display_name # 中文名称
        name # 缩写简称
        start_date # 上市日期
        end_date # 退市日期，如果没有退市则为2200-01-01
        type # 类型，stock(股票)
    """
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


def save_stock_k_all():
    connection = PooledDB.connection()
    cursor = connection.cursor()
    cursor.execute("SELECT `code_str` FROM stock_basic;")
    cord_strs = cursor.fetchall()
    for cord_str in cord_strs:
        save_stock_k(cord_str[0])
    cursor.close()
    connection.close()
    print("股票日K线更新完成")
    pass


def save_stock_k(code_str: str):
    """
        保存k线到数据库
        默认获取所有的k线图
        会自动append最新数据
    :param code_str: 股票代码 '000002.XSHE'
    :return:
    """
    connection = PooledDB.connection()
    cursor = connection.cursor()
    insert_i = 0
    try:
        table_name = "stock_k_day_" + code_str
        create_table_sql = """
    Create Table If Not Exists `stock_k_day_%s` (
      `index` datetime NOT NULL,
      `open` double NOT NULL,
      `close` double DEFAULT NULL,
      `high` double DEFAULT NULL,
      `low` double DEFAULT NULL,
      `volume` double DEFAULT NULL,
      `money` double DEFAULT NULL,
      PRIMARY KEY (`index`),
      KEY `ix_stock_k_day_index` (`index`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
    """ % (code_str)

        query_sql = "SELECT * FROM stock_basic WHERE `code_str` like %s "
        query_k_max_time = "SELECT MAX(`index`) from `" + table_name + "`"
        # 没有表则创建表
        cursor.execute(create_table_sql)
        # 找出最后更新的time
        cursor.execute(query_k_max_time)
        last_time = cursor.fetchone()[0]
        # 获取股票基础数据
        cursor.execute(query_sql, code_str)
        query_base_stock = cursor.fetchone()
        start_date = None
        end_data: timestamp = query_base_stock[4]
        if last_time is not None:
            start_date = (last_time + datetime.timedelta(days=1))
        else:
            start_date = query_base_stock[3]
        if start_date.date() > datetime.datetime.now().date():
            print("股票没有信息更新 code:%s" % code_str)
            return
        if datetime.datetime.now() > end_data:
            print("股票st code:%s" % code_str)
            return
        stock_k: pandas.DataFrame = get_price(code_str, start_date=start_date, end_date=end_data,
                                              frequency='daily',
                                              fields=None, skip_paused=False,
                                              fq='pre', count=None, panel=True, fill_paused=True)
        # engine = create_engine(f'mysql+pymysql://root:root@127.0.0.1:3306/stock')
        # stock_k.to_sql(name="stock_k_day", con=engine)
        for index in tuple(stock_k.index):
            data = stock_k.loc[index]
            # 更新插入数据
            params = tuple(data)
            if math.isnan(params[0]):
                continue
            insert_sql_k = "INSERT INTO `" + table_name + "`(`index`,`open`,`close`,`high`,`low`,`volume`,`money`) VALUES('%s',  '%s' , '%s' , '%s' , '%s' ,'%s' ,'%s')" \
                           % (str(index), params[0], params[1], params[2], params[3], params[4], params[5])
            cursor.execute(insert_sql_k, )
            insert_i += 1
            if insert_i % 50 == 0:
                connection.commit()
            pass
    finally:
        connection.commit()
        cursor.close()
        connection.close()
        if insert_i > 0:
            print("%s日k线图新增输数据%s:" % (code_str, insert_i))
        pass


def update_and_insert_data():
    """
    每日查看是否有数据更新
    """
    all_data: pandas.DataFrame = get_all_securities(types=['stock'], date=None)
    connection = PooledDB.connection()
    cursor = connection.cursor()
    update_sql = "UPDATE stock_basic SET `display_name` = %s, `name` = %s ,`start_date` = %s ,`end_date`= %s,`type` = %s WHERE `code_str` =  %s "
    insert_sql = "INSERT INTO stock_basic(`code_str`,`display_name`,`name`,`start_date`,`end_date`,`type`) VALUES(%s, %s ,  %s , %s , %s ,%s) "
    query_sql = "SELECT * FROM stock_basic WHERE `code_str` like %s "
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


def save_base_found(code_str: str):
    connection = mysql_connection_pool("stock_found").connection()
    cursor = connection.cursor()
    cursor.execute("select * from base_found where code = %s ", code_str)
    base_found_data = cursor.fetchone()
    if base_found_data is not None:
        base_found_data = {"code": base_found_data[0], "display_name": base_found_data[1],
                           "end_date": base_found_data[2],
                           "name": base_found_data[3], "start_date": base_found_data[4], "type": base_found_data[5]}
        return base_found_data
    base_found_data = get_security_info(code_str)
    cursor.execute(
        "INSERT INTO `base_found` (`code`,`display_name`,`end_date`,`name`,`start_date`,`type`) VALUES (%s,%s,%s,%s,%s,%s)",
        (base_found_data.code, base_found_data.display_name, base_found_data.end_date, base_found_data.name,
         base_found_data.start_date, base_found_data.type))
    connection.commit()
    cursor.close()
    connection.close()
    base_found_data = {"code": base_found_data.code, "display_name": base_found_data.display_name,
                       "end_date": base_found_data.end_date,
                       "name": base_found_data.name, "start_date": base_found_data.start_date,
                       "type": base_found_data.type}
    return base_found_data


def __main__():
    i = 0
    while i < 13:
        cf = configparser.ConfigParser()
        cf.read("../../config/config.properties", encoding='UTF-8')
        try:
            # 必须登录
            auth(cf.get("config%s" % i, "username%s" % i), cf.get("config%s" % i, "password%s" % i))
            print("login success")

            # 更新股票基础数据
            # update_and_insert_data()
            # 保存K线数据
            # save_stock_k_all()
            # 基金数据
            found_name = '510300.XSHG'
            base_found_data = save_base_found(found_name)
            found_data_k_data = get_price(found_name, start_date=base_found_data['start_date'],
                                          end_date=base_found_data['end_date'],
                                          frequency='daily',
                                          fields=None,
                                          skip_paused=False, fq='pre')
            """ open 时间段开始时价格
                close 时间段结束时价格
                low 最低价
                high 最高价
                volume 成交的基金数量
                money 成交的金额
                factor 前复权因子, 我们提供的价格都是前复权后的, 但是利用这个值可以算出原始价格, 方法是价格除以factor, 比如: close/factor
                high_limit 涨停价
                low_limit 跌停价
                avg 这段时间的平均价, 等于money/volume
                pre_close 前一个单位时间结束时的价格, 按天则是前一天的收盘价, 按分钟这是前一分钟的结束价格
                paused 布尔值, 这只基金是否停牌, 停牌时open/close/low/high/pre_close依然有值,都等于停牌前的收盘价, volume=money=0"""
            create_table_sql = """
                            Create Table If Not Exists `found_k_day_%s` (
                              `index` datetime NOT NULL,
                              `open` double NOT NULL,
                              `close` double DEFAULT NULL,
                              `high` double DEFAULT NULL,
                              `low` double DEFAULT NULL,
                              `volume` double DEFAULT NULL,
                              `money` double DEFAULT NULL,
                              PRIMARY KEY (`index`),
                              KEY `ix_stock_k_day_index` (`index`)
                            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
                            """ % (found_name)
            connection = mysql_connection_pool("stock_found").connection()
            cursor = connection.cursor()
            cursor.execute(create_table_sql)
            i = 0
            for index in found_data_k_data.index:
                found_data_k_row_data = found_data_k_data.loc[index]
                params = tuple(found_data_k_row_data)
                insert_sql_k = "INSERT INTO `found_k_day_" + found_name + "`(`index`,`open`,`close`,`high`,`low`,`volume`,`money`) VALUES('%s',  '%s' , '%s' , '%s' , '%s' ,'%s' ,'%s')" \
                               % (str(index), params[0], params[1], params[2], params[3], params[4], params[5])
                cursor.execute(insert_sql_k)
                i += 1
                if i % 50 == 0:
                    connection.commit()
            connection.commit()
            connection.close()
            return
        except Exception:
            traceback.print_exc()
            pass
        finally:
            i += 1
            pass


if __name__ == '__main__':
    __main__()
