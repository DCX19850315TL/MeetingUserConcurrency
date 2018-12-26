#!/usr/bin/env python
#_*_ coding:utf-8 _*_
'''
@author: tanglei
@contact: tanglei_0315@163.com
@file: logger.py
@time: 2018/12/21 10:34
'''
import os
import logging
import logging.config

def logger():
    filepath = os.path.join(os.path.abspath('conf'), 'logger.conf')
    logging.config.fileConfig(filepath)
    logger = logging.getLogger('root')
    return logger