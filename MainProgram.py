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
import sqlite3
import influxdb
from datetime import datetime, date, timedelta
import time
#自定义模块
from conf import seeting
#定义数据库的路径，绝对路径
db_path = seeting.DATABASES['NAME']
#定义企业名称
entName1 = seeting.entName1['NAME']
#用户类型
UserType = seeting.type['TYPE']
#定时时间间隔
TimeInterval = seeting.TimeInterval['TIME']

#当天的00:00:00点时间戳
def TodayTimeStamp():

    timevalue = "00:00:00"
    today = (date.today() + timedelta()).strftime("%Y-%m-%d")
    todaytime = today+timevalue
    todaytimestamp = time.strptime(todaytime,"%Y-%m-%d%H:%M:%S")
    todaytimestamp = int(time.mktime(todaytimestamp))
    return todaytimestamp

#查询并发人数的时间戳
def UserConcurrencyTimeStamp():

    todaytime = TodayTimeStamp()
    startprogramtime = int(time.time())
    pointnumber = int((startprogramtime-todaytime) / TimeInterval)
    userconcurrencytimestamp = todaytime + (TimeInterval * pointnumber)
    return userconcurrencytimestamp

#查询指定时间点并发人数
def SetDayConcurrencyNew(begTime):

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    sql = "select count(distinct userId) from confReport where cnfEntName='{0}' and (userId not in (55,59,63,75,77,79))and begTS<'{1}' and LastTS>'{1}'".format(entName1,begTime)
    date = cur.execute(sql)
    UserConcurrencyNumber = cur.fetchall()
    cur.close()
    conn.close()
    UserConcurrencyNumber = UserConcurrencyNumber[0].__str__().strip('(').strip(')').strip(',')
    return (UserConcurrencyNumber,begTime)

#将名称，企业信息，并发人数和时间写入到Influxdb
def SetUserIntoInfluxdb(json_body):

    client = influxdb.InfluxDBClient('localhost','8086','','','UserConcurrencyNumber')
    client.write_points(json_body)

if __name__ == '__main__':
    try:
        while True:
            begConcurrencyTime = UserConcurrencyTimeStamp()
            result = SetDayConcurrencyNew(begConcurrencyTime)
            Concurrency = int(result[0])
            Time = int(result[1]) * 1000000000
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
                            #"value":100
                        }
                    }
                ]
                SetUserIntoInfluxdb(json_body)
                Time = Time+TimeInterval
                time.sleep(TimeInterval)
            else:
                pass
    except Exception,e:
        print e