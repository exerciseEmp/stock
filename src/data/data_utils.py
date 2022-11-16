"""
从数据库获取数据并整理
"""
import datetime
import time
from utils.DBUtils import *
import pandas as pd


def get_single_price(code_str: str, k: str, starDate, endDate, db: str):
    """
    :param code_str:
    :param k:
    :param starDate:
    :param endDate:
    :return:dataframe
    """
    # 目前只支持日线 周线 月线 年线等明天再写
    str = "daily"
    results = None
    table_name = ''
    if "found" in db:
        table_name = "found_k_day_" + code_str
    else:
        table_name = "stock_k_day_" + code_str
    conection = mysql_connection_pool(db).connection()
    cursor = conection.cursor()
    try:
        cursor.execute("""
        SELECT * FROM `%s`
         WHERE `index` >= '%s' AND `index` <= '%s'
        """ % (table_name, starDate, endDate))
        query_data = cursor.fetchall()
        columnDes = cursor.description  # 获取连接对象的描述信息
        columnNames = [columnDes[i][0] for i in range(len(columnDes))]
        results = pd.DataFrame([list(i) for i in query_data], columns=columnNames)
        time_long = 20 * 24 * 60 * 60 * 1000

        if results.iloc[0]["index"].value / 1000000000 - time.mktime(datetime.datetime.combine(starDate.today(),
                                                                                         datetime.datetime.min.time()).timetuple()) > time_long or \
                results.iloc[len(results) - 1]["index"].value / 1000000000 - time.mktime(
            datetime.datetime.combine(endDate.today(),
                                      datetime.datetime.min.time()).timetuple()) > time_long:
            print("数据不全。。。。。。。。code_str:" + code_str)
    finally:
        cursor.close()
        conection.close()
    return results

# get_single_price("000048.xshe", None, "2015-01-16 00:00:00", "2022-02-05 00:00:00")
