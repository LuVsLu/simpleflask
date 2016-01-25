#encoding:utf-8
import time
import MySQLdb
import json
import os

import redis

from flask import Flask
from flask import abort
from flask import request

from flask.ext.restful import Api, Resource
#from flask.ext.restful import reqparse
#from flask.ext.restful import fields, marshal

app = Flask(__name__)
cors = CORS(app, resources={r"/api/*": {"origins": "*"}})
api = Api(app)

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
    return response

def connect_mysql():
    conn = MySQLdb.connect("localhost","root","456123","sms")
    return conn

def connect_redis():
    conn = redis.Redis(host='localhost', port=6379, db=0)
    return conn

def insert_task_record(taskid, tasktype, tasktotal):
    conn =  connect_mysql() 
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)

    sql = "select count(*) as total from sample_info where sample_type = '%s'"%(tasktype)
    #sql = "select count(*) as total from sample_info where sample_type = 'pdf'"
    cursor.execute(sql)
    result = cursor.fetchall()
    total = str(result[0]['total'])

    taskname = "task_"+tasktype
    if tasktotal == "all":
        sql = "insert into sample_task(task_id,task_name,vm_id,count,total,type) \
                values (%s,'%s','%s',%s,%s,'%s')"%(taskid,taskname,0,0,total,tasktype)
    else:
        sql = "insert into sample_task(task_id,task_name,vm_id,count,total,type) \
                values (%s,'%s','%s',%s,%s,'%s')"%(taskid,taskname,0,0,tasktotal,tasktype)

    print sql
    cursor.execute(sql)
    cursor.close()
    conn.close()

def start_queue_watcher(taskid, sampletype, samplenum):
    cmd = "python taskWatcher.py %s %s %s &"%(taskid, sampletype, samplenum)
    os.system(cmd)
    return

def start_queue_watcher_test(taskid, sampletype, samplenum):
    cmd = "python taskWatcherTest.py %s %s %s &"%(taskid, sampletype, samplenum)
    os.system(cmd)
    return

def insert_sample_record(data):
    conn =  connect_mysql() 
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    for item in data:
        sql = "insert into sample_info(sample_name,sample_path,sample_type,sample_date,sample_size,sample_tag) \
               values ('%s','%s','%s','%s','%s',0)"%\
               (item['Local file'],item['Remote file_id'],item['type'],item['time'],item['Uploaded size'])
        #print sql
	cursor.execute(sql)
    cursor.close()
    conn.close()

def get_task_list():
    conn =  connect_mysql() 
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    sql = "select task_id,task_name,type from sample_task group by task_id"
    cursor.execute(sql)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def get_sample_stat():
    conn =  connect_mysql() 
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    sql = "select count(*) as sample_count, sample_type from sample_info group by sample_type"
    cursor.execute(sql)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

def get_task_stat(tag):
    #读取redis里的统计信息
    r = connect_redis()
    tasklist = []
    if tag == 'all':
        tasklist = r.lrange('tasklist', 0, -1)
    else:        
        tasklist.append(tag)

    result = []
    for item in tasklist:
        stat = {}
        tmp = r.hgetall("stat_"+item)
        stat['taskid'] = item
        stat['total'] = tmp['total']
        stat['count'] = tmp['count']
        stat['percent'] = float(int(tmp['total']) - int(tmp['count'])) / float(tmp['total']) * 100
        stat['speed'] = float(tmp['count']) / (float(tmp['update']) - float(tmp['create']))
        result.append(stat)
    return result

def get_sample_queue(task_id, num):

    #连接redis
    r = connect_redis()

    #获得已经读取的数量，设置片段尾
    count = r.hget("stat_"+task_id,"count")
    end = int(num) - 1
	
    #获得[0:end]的样本列表
    result = r.lrange(task_id, 0, end)

    if len(result) == 0:
        #删除任务队列，删除队列统计信息
        r.delete(task_id)
        r.delete("stat_"+task_id)
    else:
        #删除已读取的任务
        r.ltrim(task_id, end+1, -1)
        #更新统计信息
        r.hset("stat_"+task_id, "count", int(count) + int(num))
        r.hset("stat_"+task_id, "update", int(time.time()))
		
    return result

class TaskListAPI(Resource):
    def get(self):
        tasks = get_task_list()
        return { 'tasks': tasks }

    def post(self):
        pass

class TaskAPI(Resource):
    def get(self, sample_type, sample_num):
        #生成任务ID，任务ID必须唯一，这里使用毫秒级的时间戳
        timestamp = int(time.time() * 1000)
		
        #这里将任务记录插入数据库
        insert_task_record(timestamp, sample_type, sample_num)

        #将新建的任务ID插入任务列表
        r = connect_redis()
        r.lpush("tasklist",timestamp)

        #这里启动队列监视服务
        #start_queue_watcher(timestamp, sample_type, sample_num)
        start_queue_watcher_test(timestamp, sample_type, sample_num)

        #返回任务ID
        return {'taskid': timestamp}

    def put(self, sample_type, sample_num):
        pass

    def delete(self, sample_type, sample_num):
        pass

class TaskStatListAPI(Resource):
    def get(self):
        result = get_task_stat("all")
        return {'data':result}

class TaskStatAPI(Resource):
    def get(self, task_id):
        result = get_task_stat(task_id)
        return {'data':result}

class SampleAPI(Resource):
    def get(self):
        pass

    def post(self):
        #if not request.json or not 'data' in request.json:
        #    abort(400)
        data_str = request.data
        data = json.loads(data_str)
        #return json.dumps(data)

        #insert sample record into mysql
        insert_sample_record(data["data"])

        return { 'msg': 'OK' }

    def delete(self):
        pass

class SampleListAPI(Resource):
    def get(self, task_id, sample_num):
        result = get_sample_queue(task_id, sample_num)
        
        return {'samplelist': result}

class SampleStatAPI(Resource):
    def get(self):
        result = get_sample_stat()
        return {'data':result}

class TestSampleListAPI(Resource):
    def get(self, sample_type, sample_num):
        conn =  connect_mysql() 
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        sql = "select sample_path from sample_info where sample_type = '%s' limit %s"%(sample_type, sample_num)
        #print sql
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        
        ret = []
	for item in result:
            ret.append(item['sample_path'])
        return {'samplelist':ret}

api.add_resource(SampleAPI, '/sms/api/v1.0/samples', endpoint = 'samples')
api.add_resource(SampleListAPI, '/sms/api/v1.0/samples/<string:task_id>/<string:sample_num>', endpoint = 'sample')
api.add_resource(SampleStatAPI, '/sms/api/v1.0/samples/stat', endpoint='sample_stats')

api.add_resource(TaskListAPI, '/sms/api/v1.0/tasks', endpoint = 'tasks')
api.add_resource(TaskStatListAPI, '/sms/api/v1.0/tasks/stat', endpoint = 'task_stats')
api.add_resource(TaskStatAPI, '/sms/api/v1.0/tasks/stat/<string:task_id>', endpoint = 'task_stat')
api.add_resource(TaskAPI, '/sms/api/v1.0/tasks/<string:sample_type>/<string:sample_num>', endpoint = 'task')

#测试读任务接口
api.add_resource(TestSampleListAPI, '/sms/api/v1.0/samples/test/<string:sample_type>/<string:sample_num>', endpoint = 'test')

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
