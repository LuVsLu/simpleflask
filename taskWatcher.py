import logging
import MySQLdb
import sys
import os
import time

from kafka.producer import SimpleProducer
from kafka.client import KafkaClient

def connect_mysql():
    conn = MySQLdb.connect("localhost","root","123456","sms")
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

    # To send messages synchronously
    client = KafkaClient('localhost:9092')
    producer = SimpleProducer(client, async=False)
    count = 0
    for item in samplist:
        # Note that the application is responsible for encoding messages to type bytes
        producer.send_messages(taskid, item['sample_path'])
        count = count + 1
        if(count > 10000):
            time.sleep(10)
            count = 0

def start_topic(taskid, tasktype, tasknum):
    
    #push_sample_metadata(taskid, tasktype, tasknum)

    #get the topic list
    cmd = "../kafka_2.11-0.9.0.0/bin/kafka-topics.sh --list --zookeeper localhost:2181"
    topics = os.popen(cmd).read()
    #print type(topics)

    if taskid in topics:
        push_sample_metadata(taskid, tasktype, tasknum)
    else:
        #create the new topic
        cmd = "../kafka_2.11-0.9.0.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 \
                --replication-factor 1 --partitions 1 --topic %s"%(taskid)
        ret = os.system(cmd)
        time.sleep(5)
        push_sample_metadata(taskid, tasktype, tasknum)

if __name__ == '__main__':
    taskid = sys.argv[1]
    tasktype = sys.argv[2]
    tasknum = sys.argv[3]
    start_topic(taskid, tasktype, tasknum)
