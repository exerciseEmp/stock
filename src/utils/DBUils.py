import pymysql
from dbutils.pooled_db import *


def mysql_connection():
    max_connections = 15  # 最大连接数
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


# 使用方式
pool = mysql_connection()
conn = pool.connection()
cur = conn.cursor()
SQL = 'select * from stock_basic;'
cur.execute(SQL)
f = cur.fetchall()
print(f)
cur.close()
conn.close()
