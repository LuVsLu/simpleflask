#coding:utf8

import MySQLdb
import sys
import os
import time

import redis

def connect_mysql():
    conn = MySQLdb.connect("localhost","root","456123","sms")
    return conn

def select_sample_record(tasktype, tasknum):
    conn =  connect_mysql() 
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    if tasknum == 'all' or tasknum == '0':
        sql = "select sample_path from sample_info where sample_type = '%s'"%(tasktype)
    else:
        sql = "select sample_path from sample_info where sample_type = '%s' limit %s"%(tasktype,
                tasknum)
    cursor.execute(sql)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def push_sample_metadata(taskid, tasktype, tasknum):
    samplist = select_sample_record(tasktype, tasknum)
    #print samplist
    
    r = redis.Redis(host='localhost', port=6379, db=0)

    #这里初始化任务队列统计信息
    r.hset("stat_"+taskid, "create", int(time.time()))
    r.hset("stat_"+taskid, "update", 0)
    r.hset("stat_"+taskid, "total", len(samplist))
    r.hset("stat_"+taskid, "count", 0)
	
    for item in samplist:
        # Note that the application is responsible for encoding messages to type bytes
        r.lpush(taskid,item['sample_path'])

if __name__ == '__main__':
    taskid = sys.argv[1]
    tasktype = sys.argv[2]
    tasknum = sys.argv[3]
    push_sample_metadata(taskid, tasktype, tasknum)
