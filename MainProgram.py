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
import numpy
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
#获取所有Relay的地址与主机名对应关系
RelayAll = conf.items("relay")
#配置项的列表个数
RelayAllLen = len(RelayAll)
#获取relay的所有section
RelayAllList = []
for item in range(RelayAllLen):
    RelayAllList.append(RelayAll[item][1])
#获取数据库的表名称
ConcurrencySeeting = conf.get("database","ConcurrencyMeasurement")

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

#计算指定时间点各Relay的并发人数
RelayResultList = []
def SetRelayConcurrencyNew(begTime):
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        sql = "select count(relayIp),relayIp from confReport where cnfEntName='{0}' and (userId not in (55,59,63,75,77,79))and begTS<'{1}' and LastTS>'{1}' GROUP BY relayIp".format(entName1,begTime)
        date = cur.execute(sql)
        RelayConcurrencyNumber = cur.fetchall()
        cur.close()
        conn.close()
        RelayConcurrencyNumberLen = len(RelayConcurrencyNumber)
        for i in range(RelayConcurrencyNumberLen):
            for j in range(RelayAllLen):
                RelayConcurrencyNumberIPaddr = RelayConcurrencyNumber[i][1]
                RelayIPaddr = RelayAll[j][1]
                if RelayConcurrencyNumberIPaddr == RelayIPaddr:
                    RelayArray = numpy.array([RelayAll[j][0],RelayConcurrencyNumber[i][0]])
                elif RelayConcurrencyNumberIPaddr not in RelayAllList:
                    RelayArray = numpy.array([RelayConcurrencyNumberIPaddr, RelayConcurrencyNumber[i][0]])
            RelayResultList.append(RelayArray)
        return (RelayResultList,begTime)
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

#将名称，企业信息/Relay主机名，并发人数和时间写入到Influxdb
def SetRelayIntoInfluxdb(json_body):
    try:
        client = influxdb.InfluxDBClient('localhost','8086','','','RelayConcurrencyNumber')
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
                #求指定时间
                begConcurrencyTime = UserConcurrencyTimeStamp()
                #设置指定时间并发人数
                result = SetDayConcurrencyNew(begConcurrencyTime)
                Concurrency = int(result[0])
                # 设置指定时间的Relay的并发人数
                relayresult = SetRelayConcurrencyNew(begConcurrencyTime)
                relayresultlen = len(relayresult[0])
                #将时间的单位从秒转换为纳秒，用于Influxdb数据库
                Time = int(result[1]) * 1000000000
                #设置日志时间，并将时间戳转换为正常的时间格式
                TimeLogStamp = int(result[1])
                TimeLogArray = time.localtime(TimeLogStamp)
                TimeLog = time.strftime("%Y-%m-%d %H:%M:%S", TimeLogArray)

                if Concurrency != 0:
                    # 数据json格式化
                    json_body = [
                        {
                            "measurement": ConcurrencySeeting,
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
                    for i in range(relayresultlen):
                        # 数据json格式化
                        json_body1 = [
                            {
                                "measurement": "Relay_Concurrency",
                                "tags": {
                                    "entName": entName1,
                                    "RelayName": relayresult[0][i][0],
                                },
                                "time": Time,
                                "fields": {
                                    "value": relayresult[0][i][1],
                                }
                            }
                        ]
                        SetRelayIntoInfluxdb(json_body1)
                        logger().info('这个%s时间点%sRelay的并发人数为%s' % (TimeLog, entName1, relayresult[0][i][1]))
                    Time = Time + TimeInterval
                    time.sleep(TimeInterval)
                else:
                    pass
        except:
            s = traceback.format_exc()
            logger().error(s)
