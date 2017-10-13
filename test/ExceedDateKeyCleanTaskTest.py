# -*- coding: utf-8 -*-
# !/usr/bin/env python

'''
测试脚本
'''

import sys,os

sys.path.append(os.path.dirname(__file__) + "/../../")
reload(sys)

from redis_clean.cleantask.ExceedDateKeyCleanTask import ExceedDateKeyCleanTask
from redis_clean.redis.RedisClient import RedisClient
from redis_clean.util.GetConfig import GetConfig
from redis_clean.util.DateUtil import DateUtil

class ExceedDateKeyCleanTaskTest(object):
  '''
  KeyCleanTask 测试类
  '''

  def __init__(self):
    self.__exceedDateKeyCleanTask = ExceedDateKeyCleanTask()
    self.__config = GetConfig()
    self.__redisClient = RedisClient(self.__config.redis_host(), self.__config.redis_port(), self.__config.redis_password())

  def testStart(self):
    keyPrefix = 'account_cost_'
    validDays = 2
    db = 1
    day = DateUtil.format4Y2m2d(DateUtil.addDaysOnCurrent(-validDays))
    key = keyPrefix + day
    print "----test start func begin"
    self.__redisClient.hset(db, key, 'map1', 'value1')
    self.__redisClient.hset(db, key, 'map2', 'value2')
    print("init data:")
    print(self.__redisClient.hscan(db, key, 0, None, 3))
    self.__exceedDateKeyCleanTask.start(keyPrefix, db, validDays)
    print("after clean:")
    print(self.__redisClient.hscan(db, key, 0, None, 3))
    pass

  def testStartHour(self):
    keyPrefix = 'data_cost_'
    validDays = 2
    db = 3
    day = DateUtil.format4Y2m2d(DateUtil.addDaysOnCurrent(-validDays))
    print "----test startHour func begin"
    keyList = []
    for i in xrange(0, 24):
      if i < 10:
        key = keyPrefix + day + '0' + str(i)
      else:
        key = keyPrefix + day + str(i)
      keyList.append(key)
      self.__redisClient.hset(db, key, 'map1', 'value1')
      self.__redisClient.hset(db, key, 'map2', 'value2')

    print("init data:")
    for key in keyList:
      print(self.__redisClient.hgetall(db, key))
    self.__exceedDateKeyCleanTask.startHour(keyPrefix, db, validDays)
    print("after clean:")
    for key in keyList:
      print(self.__redisClient.hgetall(db, key))
    pass

  def testStartBatch(self):
    keyPrefix = 'mix_detail_posid_'
    validDays = 30
    db = 1
    day = DateUtil.format4Y2m2d(DateUtil.addDaysOnCurrent(-validDays))
    key = keyPrefix + day
    print "----test startBatch func begin"
    self.__redisClient.sadd(db, key, *['mem1', 'mem2'])
    print("init data:")
    print(self.__redisClient.sscan(db, key, 0, None, 3))
    self.__exceedDateKeyCleanTask.startBatch(keyPrefix, db, validDays)
    print("after clean:")
    print(self.__redisClient.sscan(db, key, 0, None, 3))
    pass

  def testStartHscan(self):
    keyPrefix = 'view_st_ts_'
    validDays = 1
    db = 1
    day = DateUtil.format4Y2m2d(DateUtil.addDaysOnCurrent(-validDays))
    key = keyPrefix + day
    print "----test startHscan func begin"
    self.__redisClient.hset(db, key, 'map1', 'value1')
    self.__redisClient.hset(db, key, 'map2', 'value2')
    print("init data:")
    print(self.__redisClient.hscan(db, key, 0, None, 3))
    self.__exceedDateKeyCleanTask.startHscan(keyPrefix, db, validDays)
    print("after clean:")
    print(self.__redisClient.hscan(db, key, 0, None, 3))
    pass

  def testStartSscan(self):
    keyPrefix = 'click_st_'
    validDays = 1
    db = 1
    day = DateUtil.format4Y2m2d(DateUtil.addDaysOnCurrent(-validDays))
    key = keyPrefix + day
    print "----test startSscan func begin"
    self.__redisClient.sadd(db, key, *['mem1', 'mem2'])
    print("init data:")
    print(self.__redisClient.sscan(db, key, 0, None, 3))
    self.__exceedDateKeyCleanTask.startSscan(keyPrefix, db, validDays)
    print("after clean:")
    print(self.__redisClient.sscan(db, key, 0, None, 3))
    pass

if __name__ == '__main__':
  taskTest = ExceedDateKeyCleanTaskTest()
  taskTest.testStart()
  taskTest.testStartHour()
  taskTest.testStartBatch()
  taskTest.testStartHscan()
  taskTest.testStartSscan()