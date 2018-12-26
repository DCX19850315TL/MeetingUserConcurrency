#!/usr/bin/env python
#_*_ coding:utf-8 _*_
'''
@author: tanglei
@contact: tanglei_0315@163.com
@file: MainProgram.py
@time: 2018/12/21 10:32
'''
#系统模块
import os
import sys
import sqlite3
import influxdb
from datetime import datetime, date, timedelta
import time
import ConfigParser
import traceback
import socket
#自定义模块
from common.logger import logger
#设置默认字符集为utf-8
reload(sys)
sys.setdefaultencoding("utf-8")
#通过ConfigParser模块进行配置文件的读取
seeting_file = os.path.join(os.path.abspath('conf'),'seeting.ini')
conf = ConfigParser.ConfigParser()
conf.read(seeting_file)
#定义数据库的路径，绝对路径
db_path = conf.get("database", "sqllite3")
#定义企业名称
entName1 = conf.get("entSeeting", "entName")
#定义用户类型
UserType = conf.get("entSeeting", "type")
#定义时间间隔
TimeInterval = int(conf.get("entSeeting", "intervaltime"))
#定义查询多长时间之前的时间
TimeBefore = int(conf.get("entSeeting","beforetime"))

#当天的00:00:00点时间戳
def TodayTimeStamp():
    try:
        timevalue = "00:00:00"
        today = (date.today() + timedelta()).strftime("%Y-%m-%d")
        todaytime = today+timevalue
        todaytimestamp = time.strptime(todaytime,"%Y-%m-%d%H:%M:%S")
        todaytimestamp = int(time.mktime(todaytimestamp))
        return todaytimestamp
    except:
        s = traceback.format_exc()
        logger().error(s)

#计算并发人数的时间戳
def UserConcurrencyTimeStamp():
    try:
        todaytime = TodayTimeStamp()
        startprogramtime = int(time.time())
        pointnumber = int((startprogramtime-todaytime) / TimeInterval)
        userconcurrencytimestamp = todaytime + (TimeInterval * pointnumber) - TimeBefore
        return userconcurrencytimestamp
    except:
        s = traceback.format_exc()
        logger().error(s)

#计算指定时间点并发人数
def SetDayConcurrencyNew(begTime):
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        sql = "select count(distinct userId) from confReport where cnfEntName='{0}' and (userId not in (55,59,63,75,77,79))and begTS<'{1}' and LastTS>'{1}'".format(entName1,begTime)
        date = cur.execute(sql)
        UserConcurrencyNumber = cur.fetchall()
        cur.close()
        conn.close()
        UserConcurrencyNumber = UserConcurrencyNumber[0].__str__().strip('(').strip(')').strip(',')
        return (UserConcurrencyNumber,begTime)
    except:
        s = traceback.format_exc()
        logger().error(s)

#将名称，企业信息，并发人数和时间写入到Influxdb
def SetUserIntoInfluxdb(json_body):
    try:
        client = influxdb.InfluxDBClient('localhost','8086','','','UserConcurrencyNumber')
        client.write_points(json_body)
    except:
        s = traceback.format_exc()
        logger().error(s)

if __name__ == '__main__':
    #连接InfluxDB的端口
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('127.0.0.1', 8086))
    #判断SqlLite3数据库是否连接成功
    if not os.path.isfile(db_path):
        print 'SqlLite3 Databases is not Connect'
        logger().error('SqlLite3数据库连接有问题')
    #判断InfluxDB数据库端口是否监听
    elif result != 0:
        print 'InfluxDB Databases port is problem'
        logger().error('InfluxDB数据库端口连接不上')
    elif os.path.isfile(db_path) and result == 0:
        print 'The database is running normally and the program is running normally'
        logger().info('数据库运行正常，程序正常工作')
        try:
            while True:
                begConcurrencyTime = UserConcurrencyTimeStamp()
                result = SetDayConcurrencyNew(begConcurrencyTime)
                Concurrency = int(result[0])
                Time = int(result[1]) * 1000000000
                TimeLogStamp = int(result[1])
                TimeLogArray = time.localtime(TimeLogStamp)
                TimeLog = time.strftime("%Y-%m-%d %H:%M:%S", TimeLogArray)
                if Concurrency != 0:
                    # 数据json格式化
                    json_body = [
                        {
                            "measurement": "Concurrency",
                            "tags": {
                                "entName": entName1,
                            },
                            "time": Time,
                            "fields": {
                                "value": Concurrency,
                            }
                        }
                    ]
                    SetUserIntoInfluxdb(json_body)
                    logger().info('这个%s时间点%s企业并发人数为%s' % (TimeLog, entName1, Concurrency))
                    Time = Time+TimeInterval
                    time.sleep(TimeInterval)
                else:
                    pass
        except:
            s = traceback.format_exc()
            logger().error(s)