# -*- coding: utf-8 -*-
# !/usr/bin/env python

'''
对于一些过期的以及不再使用的key进行删除,防止redis无用键越来越多
'''

import sys,os

sys.path.append(os.path.dirname(__file__) + "/../../")
reload(sys)

from redis_clean.redis.RedisClient import RedisClient
from redis_clean.util.GetConfig import GetConfig
from redis_clean.util.Logger import Logger
from redis_clean.util.DateUtil import DateUtil

class KeyClearTask(object):
  '''
  对于一些过期的以及不再使用的key进行删除
  '''

  def __init__(self):
    self.__config = GetConfig()
    self.__log = Logger('exceedDateKeyCleanTask')
    self.__redisClient = RedisClient(self.__config.redis_host(), self.__config.redis_port(), self.__config.redis_password())
    pass

  def doTask(self):
    '''
    清理指定的key
    :return:
    '''
    dayList = DateUtil.splitIntoDays(DateUtil.addDaysOnCurrent(-3), DateUtil.getCurrentDate())
    hourList = DateUtil.splitIntoHours(DateUtil.addHoursOnCurrent(-6), DateUtil.getCurrentDate())

    unlinkKeyList = []

    # 删除以day为单位的key
    for day in dayList:
      unlinkKeyList.append('launch_date_' + day) # 投放日期(Set),存储campaign Id 的set(白名单)
      unlinkKeyList.append('launch_date_2_creatives_' + day) # 投放日期到creative的映射关系,存储的是BitSet
      unlinkKeyList.append('fingerprint_2_creativeId_' + day) # fingerprint与creativeId对应关系

    # 删除以hour为单位的key
    for hour in hourList:
      unlinkKeyList.append('account_unfreeze_time_' + hour) # 广告商账户级别的冻结(预算不足),该小时冻结,存储的是agentId set
      unlinkKeyList.append('campaign_unfreeze_time_' + hour) # 推广计划级别的冻结(预算不足),该小时冻结,存储的是campaign Id set(黑名单)
      unlinkKeyList.append('account_unfreeze_time_2_creatives_' + hour) # 账户级别的冻结时间到creative的映射关系,存储的是BitSet
      unlinkKeyList.append('campaign_unfreeze_time_2_creatives_' + hour) # 推广计划级别的冻结时间到creative的映射关系,存储的是BitSet

    self.__redisClient.unlink(0, *unlinkKeyList)
    self.__log.info("unlink keys:{keys}".format(keys=unlinkKeyList.__str__()))

if __name__ == '__main__':
  keyCleanTask = KeyClearTask()
  keyCleanTask.doTask()