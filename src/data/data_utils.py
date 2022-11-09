"""
从数据库获取数据并整理
"""
import utils.DBUtils as db_utils
import pandas as pd


def get_single_price(code_str: str, k: str, starDate, endDate):
    """
    :param code_str:
    :param k:
    :param starDate:
    :param endDate:
    :return:dataframe
    """
    # 目前只支持日线 周线 月线 年线等明天再写
    str = "daily"
    conection = db_utils.PooledDB.connection()
    cursor = conection.cursor()
    results = None
    try:
        cursor.execute("""
        SELECT * FROM `stock_k_day_%s`
         WHERE `index` >= '%s' AND `index` <= '%s'
        """ % (code_str, starDate, endDate))
        query_data = cursor.fetchall()
        columnDes = cursor.description  # 获取连接对象的描述信息
        columnNames = [columnDes[i][0] for i in range(len(columnDes))]
        results = pd.DataFrame([list(i) for i in query_data], columns=columnNames)
    finally:
        cursor.close()
        conection.close()
    return results


# get_single_price("000048.xshe", None, "2015-01-16 00:00:00", "2022-02-05 00:00:00")
