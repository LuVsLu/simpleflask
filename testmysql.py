#encoding:utf-8
import MySQLdb

def connect_mysql():
    conn = MySQLdb.connect("localhost","root","123456","sms")
    return conn

def insert_task_record():
    conn =  connect_mysql() 
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    sql = "select sample_path from sample_info" 
    cursor.execute(sql)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

print insert_task_record()
