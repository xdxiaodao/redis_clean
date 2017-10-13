# -*- coding: utf-8 -*-
# !/usr/bin/env python

'''
测试脚本
'''

import sys,os

sys.path.append(os.path.dirname(__file__) + "/../../")
reload(sys)

from redis_clean.cleantask.KeyCleanTask import KeyClearTask
from redis_clean.redis.RedisClient import RedisClient
from redis_clean.util.GetConfig import GetConfig
from redis_clean.util.DateUtil import DateUtil

class KeyCleanTaskTest(object):
  '''
  KeyCleanTask 测试类
  '''

  def __init__(self):
    self.__keyCleanTask = KeyClearTask()
    self.__config = GetConfig()
    self.__redisClient = RedisClient(self.__config.redis_host(), self.__config.redis_port(), self.__config.redis_password())

  def testDoTask(self):
    dayList = DateUtil.splitIntoDays(DateUtil.addDaysOnCurrent(-3), DateUtil.getCurrentDate())
    hourList = DateUtil.splitIntoHours(DateUtil.addHoursOnCurrent(-6), DateUtil.getCurrentDate())
    db = 0
    print "----test doTask func begin"
    unlinkKeyList = []
    unlinkSetKeyList = []

    # 删除以day为单位的key
    for day in dayList:
      unlinkSetKeyList.append('launch_date_' + day) # 投放日期(Set),存储campaign Id 的set(白名单)
      unlinkKeyList.append('launch_date_2_creatives_' + day) # 投放日期到creative的映射关系,存储的是BitSet
      unlinkKeyList.append('fingerprint_2_creativeId_' + day) # fingerprint与creativeId对应关系

      self.__redisClient.sadd(db, 'launch_date_' + day, *['mem1', 'mem2'])
      self.__redisClient.set(db, 'launch_date_2_creatives_' + day, 'launch_date_2_creatives_value1')
      self.__redisClient.set(db, 'fingerprint_2_creativeId_' + day, 'fingerprint_2_creativeId_value1')

    # 删除以hour为单位的key
    for hour in hourList:
      unlinkKeyList.append('account_unfreeze_time_' + hour) # 广告商账户级别的冻结(预算不足),该小时冻结,存储的是agentId set
      unlinkKeyList.append('campaign_unfreeze_time_' + hour) # 推广计划级别的冻结(预算不足),该小时冻结,存储的是campaign Id set(黑名单)
      unlinkKeyList.append('account_unfreeze_time_2_creatives_' + hour) # 账户级别的冻结时间到creative的映射关系,存储的是BitSet
      unlinkKeyList.append('campaign_unfreeze_time_2_creatives_' + hour) # 推广计划级别的冻结时间到creative的映射关系,存储的是BitSet

      self.__redisClient.set(db, 'account_unfreeze_time_' + hour, 'account_unfreeze_time_')
      self.__redisClient.set(db, 'campaign_unfreeze_time_' + hour, 'campaign_unfreeze_time_')
      self.__redisClient.set(db, 'account_unfreeze_time_2_creatives_' + hour, 'account_unfreeze_time_2_creatives_')
      self.__redisClient.set(db, 'campaign_unfreeze_time_2_creatives_' + hour, 'campaign_unfreeze_time_2_creatives_')

    print("init data:")
    print(self.__redisClient.mget(db, *unlinkKeyList))
    for unlinkSetKey in unlinkSetKeyList:
      print(self.__redisClient.sscan(db, unlinkSetKey, 0, None, 3))
    self.__keyCleanTask.doTask()
    print("after clean:")
    print(self.__redisClient.mget(db, *unlinkKeyList))
    for unlinkSetKey in unlinkSetKeyList:
      print(self.__redisClient.sscan(db, unlinkSetKey, 0, None, 3))

if __name__ == '__main__':
  taskTest = KeyCleanTaskTest()
  taskTest.testDoTask()