import multiprocessing

import pymysql
from dbutils.pooled_db import *
from multiprocessing import cpu_count


def mysql_connection_pool() -> PooledDB:
    max_connections = multiprocessing.cpu_count() * 2 + 1  # 最大连接数
    connection_pool = PooledDB(
        pymysql,
        max_connections,
        host='localhost',
        user='root',
        port=3306,
        passwd='root',
        db='stock',
        use_unicode=True)
    return connection_pool


PooledDB = mysql_connection_pool()





# 使用方式
# pool = mysql_connection()
# conn = PooledDB.connection()
# conn = PooledDB.connection()
# cur = conn.cursor()
# SQL = 'select * from stock_basic;'
# cur.execute(SQL)
# f = cur.fetchall()
# print(f)
# cur.close()
# conn.close()
