# -*- coding: utf-8 -*-
# !/usr/bin/env python

'''
删除超过N天的数据
'''

import time
import sys,os

sys.path.append(os.path.dirname(__file__) + "/../../")
reload(sys)

from redis_clean.redis.RedisClient import RedisClient
from redis_clean.util.GetConfig import GetConfig
from redis_clean.util.Logger import Logger
from redis_clean.util.DateUtil import DateUtil

class ExceedDateKeyCleanTask(object):
  '''
  删除超过N天数据的清理任务
  '''

  def __init__(self):
    self.__config = GetConfig()
    self.__log = Logger('exceedDateKeyCleanTask')
    self.__redisClient = RedisClient(self.__config.redis_host(), self.__config.redis_port(), self.__config.redis_password())
    self.__taskName = 'clean exceed date redis key'
    self.__className = self.__class__

  def __doStart(self, keyPrefix, db, date):
    startTime = time.time()
    taskName = self.__taskName + " by doStart"
    self.__log.info("start doing task:{task}, className:{className}".format(task=taskName, className=self.__class__))
    key = keyPrefix + date;
    self.__redisClient.unlink(db, key);
    self.__log.info("unlink key:{key}".format(key=key))
    costTime = time.time() - startTime
    self.__log.info("task:{task} finish, className:{className}, key:{key}, db:{db}, cost:{cost}" \
                    .format(task=taskName, className=self.__className, key=key, db=db, cost=costTime))

  def start(self, keyPrefix, db, validDays):
    """
    删除指定前缀的key
    :param keyPrefix 待删除的key前缀 key的格式为 keyPrefix_yyyyMMdd,其中前缀为**_
    :param db key所在的数据库
    :param validDays key在几天内有效
    :return:
    """
    day = DateUtil.format4Y2m2d(DateUtil.addDaysOnCurrent(-validDays))
    self.__doStart(keyPrefix, db, day)
    pass

  def startHour(self, keyPrefix, db, validDays):
    """
    清除时间格式为小时的key，即key的格式为keyPrefix_yyyyMMddHH
    :param keyPrefix 待删除的key前缀 key的格式为 keyPrefix_yyyyMMdd,其中前缀为**_
    :param db key所在的数据库
    :param validDays key在几天内有效
    :return:
    """
    day = DateUtil.format4Y2m2d(DateUtil.addDaysOnCurrent(-validDays))
    for i in xrange(0, 24):
      if i < 10:
        self.__doStart(keyPrefix, db, day + '0' + str(i))
      else:
        self.__doStart(keyPrefix, db, day + str(i))


  def startBatch(self, keyPrefix, db, validDays):
    """
    删除指定前缀的set中保存的所有value对应的key（set中保存的就是要删除的key）
    :param keyPrefix 待删除的key前缀 key的格式为 keyPrefix_yyyyMMdd,其中前缀为**_
    :param db key所在的数据库
    :param validDays key在几天内有效
    :return:
    """
    startTime = time.time()
    taskName = self.__taskName + " by startBatch"
    self.__log.info("start doing task:{task}, className:{className}".format(task=taskName, className=self.__className))
    day = DateUtil.format4Y2m2d(DateUtil.addDaysOnCurrent(-validDays))
    setKey = keyPrefix + day
    cursor = 0

    # 每次删除1000条记录
    while True:
      scanResult = self.__redisClient.sscan(db, setKey, cursor, None, 1000)
      # 如果获取不到记录，则跳出循环
      if len(scanResult) == 0:
        break
      cursor = scanResult[0]

      # 删除记录
      if len(scanResult[1]) > 0:
        self.__redisClient.unlink(db, *scanResult[1])
        self.__redisClient.srem(db, setKey, *scanResult[1])

      # 如果已经检索完，则跳出循环
      if cursor == 0:
        break

    costTime = time.time() - startTime
    self.__log.info("task:{task} finish, className:{className}, key:{key}, db:{db}, cost:{cost}" \
                    .format(task=taskName, className=self.__className, key=setKey, db=db, cost=costTime))

  def startHscan(self, keyPrefix, db, validDays):
    """
    使用hscan删除大数据量的hash key
    :param keyPrefix 待删除的key前缀 key的格式为 keyPrefix_yyyyMMdd,其中前缀为**_
    :param db key所在的数据库
    :param validDays key在几天内有效
    :return:
    """
    startTime = time.time()
    taskName = self.__taskName + " by startHscan"
    self.__log.info("start doing task:{task}, className:{className}".format(task=taskName, className=self.__className))
    day = DateUtil.format4Y2m2d(DateUtil.addDaysOnCurrent(-validDays))
    key = keyPrefix + day
    cursor = 0

    # 每次删除1000条记录
    while True:
      scanResult = self.__redisClient.hscan(db, key, cursor, None, 1000)
      # 如果获取不到记录，则跳出循环
      if len(scanResult) == 0:
        break
      cursor = scanResult[0]

      # 获取map中的所有key
      keyList = []
      if len(scanResult[1]) > 0:
        for (k, v) in scanResult[1].items():
          keyList.append(k)

      # 批量删除记录
      self.__redisClient.hdel(db, key, *keyList)

      # 如果已经检索完，则跳出循环
      if cursor == 0:
        break

    costTime = time.time() - startTime
    self.__log.info("task:{task} finish, className:{className}, key:{key}, db:{db}, cost:{cost}" \
                    .format(task=taskName, className=self.__className, key=key, db=db, cost=costTime))

  def startSscan(self, keyPrefix, db, validDays):
    """
    使用sscan删除大数据量的set key
    :param keyPrefix 待删除的key前缀 key的格式为 keyPrefix_yyyyMMdd,其中前缀为**_
    :param db key所在的数据库
    :param validDays key在几天内有效
    :return:
    """
    startTime = time.time()
    taskName = self.__taskName + " by startSscan"
    self.__log.info("start doing task:{task}, className:{className}".format(task=taskName, className=self.__className))
    day = DateUtil.format4Y2m2d(DateUtil.addDaysOnCurrent(-validDays))
    key = keyPrefix + day
    cursor = 0

    # 每次删除1000条记录
    while True:
      scanResult = self.__redisClient.sscan(db, key, cursor, None, 1000)
      # 如果获取不到记录，则跳出循环
      if len(scanResult) == 0:
        break
      cursor = scanResult[0]

      # 删除记录
      if len(scanResult[1]) > 0:
        self.__redisClient.srem(db, key, *scanResult[1])

      # 如果已经检索完，则跳出循环
      if cursor == 0:
        break

    costTime = time.time() - startTime
    self.__log.info("task:{task} finish, className:{className}, key:{key}, db:{db}, cost:{cost}" \
                    .format(task=taskName, className=self.__className, key=key, db=db, cost=costTime))

if __name__ == '__main__':
  # 根据参数调用对应的函数
  if len(sys.argv) < 5:
    print("invalid param, usage: python ExceedDateKeyCleanTask.py start|startHour|startBatch|startHscan|startSscan keyPrefix db validDays")
    quit(1)

  try:
    funcName = sys.argv[1]
    keyPrefix = sys.argv[2]
    db = int(sys.argv[3])
    validDays = int(sys.argv[4])
  except BaseException:
    print("param error!")
    print("param keyPrefix length must be in [1-255]")
    print("param db length must be in [0-15]")
    print("param validDays length must be in [1-100]")
    quit(-1)


  if None == keyPrefix or len(keyPrefix) > 255 or len(keyPrefix) == 0:
    print("param keyPrefix length must be in [1-255]")
    quit(2)
  if None == db or db not in range(0, 16):
    print("param db length must be in [0-15]")
    quit(3)
  if None == validDays or validDays > 100 or validDays <= 0:
    print("param validDays length must be in [1-100]")
    quit(4)

  exceedDateKeyCleanTask = ExceedDateKeyCleanTask()
  if funcName == 'start':
    exceedDateKeyCleanTask.start(keyPrefix, db, validDays)
  elif funcName == 'startHour':
    exceedDateKeyCleanTask.startHour(keyPrefix, db, validDays)
  elif funcName == 'startBatch':
    exceedDateKeyCleanTask.startBatch(keyPrefix, db, validDays)
  elif funcName == 'startHscan':
    exceedDateKeyCleanTask.startHscan(keyPrefix, db, validDays)
  elif funcName == 'startSscan':
    exceedDateKeyCleanTask.startSscan(keyPrefix, db, validDays)
  else:
    pass


