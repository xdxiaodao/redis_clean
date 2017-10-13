# -*- coding: utf-8 -*-
# !/usr/bin/env python

'''
redis 客户端
'''

import sys,os

import redis

sys.path.append(os.path.dirname(__file__) + "/../../")
reload(sys)

from redis_clean.util.Logger import Logger


class RedisClient(object):
  """
  Reids client
  """

  def __init__(self, host, port, password):
    """
    init
    :param host:
    :param port:
    :param password:
    :return:
    """
    self.__host = host
    self.__port = port
    self.__password = password
    self.__conn_pool_dict = {}
    self.__log = Logger('redisClient')

  def __getClient(self, db):
    if db not in self.__conn_pool_dict:
      conn_pool = redis.ConnectionPool(host=self.__host, port=self.__port, password=self.__password, db=db)
      self.__conn_pool_dict[db] = conn_pool

    conn_pool = self.__conn_pool_dict.get(db)
    return redis.StrictRedis(connection_pool=conn_pool)

  def unlink(self, db, *keys):
    """
    unlink,
    :param db: 数据库
    :param keys: 待删除的key，可变参数
    :return:
    """
    try:
      mergeList = ['unlink']
      for key in keys:
        mergeList.append(key)
      client = self.__getClient(db)
      client.execute_command(*mergeList)
    except BaseException:
      self.__log.info("unlink has some error, unlink keys:{keys}".format(keys=keys.__str__()))

  def sscan(self, db, name, cursor, match, count):
    """
    sscan, set扫描
    :param db: 数据库
    :param name: 待扫描的key
    :param cursor: 游标起点
    :param match: 匹配
    :param count: 扫描的个数
    :return:
    """
    try:
      client = self.__getClient(db)
      return client.sscan(name, cursor, match, count)
    except BaseException, IOError:
      self.__log.info("sscan has some error, sscan name:{name}".format(name=name))
      return {}

  def srem(self, db, name, *values):
    """
    srem, set删除
    :param db: 数据库
    :param name: set的key
    :param values: set中的数据，可变参数
    :return:
    """
    try:
      client = self.__getClient(db)
      client.srem(name, *values)
    except BaseException:
      self.__log.error("srem has some error, srem name:{name}, values:{values}".format(name=name, values=values.__str__()))

  def hscan(self, db, name, cursor, match, count):
    """
    hscan, set扫描
    :param db: 数据库
    :param name: 待扫描的key
    :param cursor: 游标起点
    :param count: 扫描的个数
    :return:
    """
    try:
      client = self.__getClient(db)
      return client.hscan(name, cursor, match, count)
    except BaseException:
      self.__log.info("hscan has some error, sscan name:{name}".format(name=name))
      return {}

  def hdel(self, db, name, *keys):
    """
    hdel, hashmap删除
    :param db: 数据库
    :param name: hashmap的key
    :param keys: hashmap中keys，可变参数
    :return:
    """
    try:
      client = self.__getClient(db)
      client.hdel(name, *keys)
    except BaseException:
      self.__log.error("hdel has some error, hdel name:{name}, keys:{keys}".format(name=name, keys=keys.__str__()))

  def set(self, db, name, value):
    """
    set, 设置key-value
    :param db: 数据库
    :param name: 的key
    :param value: 值
    :return:
    """
    try:
      client = self.__getClient(db)
      client.set(name,value)
    except BaseException:
      self.__log.error("set has some error, name:{name},value:{value}".format(name=name, value=value))

  def sadd(self, db, name, *values):
    """
    sadd, 设置key-value
    :param db: 数据库
    :param name: 的key
    :param values: set中的值
    :return:
    """
    try:
      client = self.__getClient(db)
      client.sadd(name, *values)
    except BaseException:
      self.__log.error("sadd has some error, name:{name},values:{values}".format(name=name, values=values.__str__()))

  def hset(self, db, name, key, value):
    """
    hset, 添加map数据
    :param db: 数据库
    :param name: 的key
    :param key: map中的key
    :param value: map中的value
    :return:
    """
    try:
      client = self.__getClient(db)
      client.hset(name, key, value)
    except BaseException:
      self.__log.error("hadd has some error, name:{name}, key:{key}, value:{value}".format(name=name, key=key, value=value))

  def hmset(self, db, name, mapping):
    """
    hset, 批量添加map数据
    :param db: 数据库
    :param name: 的key
    :param mapping: <key,value>
    :return:
    """
    try:
      client = self.__getClient(db)
      client.hmset(name, mapping)
    except BaseException:
      self.__log.error("hmset has some error, name:{name}, mapping:{mapping}".format(name=name, mapping=mapping))

  def get(self, db, name):
    """
    获取value值
    :param db: 数据库
    :param name: key
    :return:
    """
    try:
      client = self.__getClient(db)
      return client.get(name)
    except BaseException:
      self.__log.error("get has some error, name:{name}".format(name=name))
      return None

  def mget(self, db, *keys):
    """
    批量获取value值
    :param db: 数据库
    :param keys: keys
    :return:
    """
    try:
      client = self.__getClient(db)
      return client.mget(*keys)
    except BaseException:
      self.__log.error("mget has some error, keys:{keys}".format(keys=keys.__str__()))
      return {}

  def hget(self, db, name, key):
    """
    获取value值
    :param db: 数据库
    :param name: hashmap的key
    :param key: key
    :return:
    """
    try:
      client = self.__getClient(db)
      return client.hget(name, key)
    except BaseException:
      self.__log.error("hget has some error, name:{name}, key:{key}".format(name=name, key=key))
      return None

  def hmget(self, db, name, *keys):
    """
    获取value值
    :param db: 数据库
    :param name: hashmap的key
    :param keys: keys
    :return:
    """
    try:
      client = self.__getClient(db)
      return client.hmget(name, *keys)
    except BaseException:
      self.__log.error("hmget has some error, name:{name}, keys:{keys}".format(name=name, keys=keys))
      return None

  def hgetall(self, db, name):
    """
    获取value值
    :param db: 数据库
    :param name: hashmap的key
    :param keys: keys
    :return:
    """
    try:
      client = self.__getClient(db)
      return client.hgetall(name)
    except BaseException:
      self.__log.error("hgetall has some error, name:{name}".format(name=name))
      return None

if __name__ == '__main__':
  redisClient = RedisClient("a.redis.sogou", "1650", "stayhungrystayfoolish")

  # unlink test
  redisClient.set(0, 'foo1', 'bar1')
  redisClient.set(0, 'foo2', 'bar2')
  redisClient.set(0, 'foo3', 'bar3')
  print(redisClient.mget(0, *['foo1', 'foo2', 'foo3']))

  redisClient.unlink(0, 'foo1')
  print(redisClient.mget(0, *['foo1', 'foo2', 'foo3']))

  redisClient.unlink(0, *['foo2', 'foo3'])
  print(redisClient.mget(0, *['foo1', 'foo2', 'foo3']))

  # set test
  redisClient.unlink(0, 'set1')
  redisClient.sadd(0, 'set1', 'value1')
  redisClient.sadd(0, 'set1', 'value2')
  redisClient.sadd(0, 'set1', 'value3')

  print(redisClient.sscan(0, 'set1', 0, None, 2))
  print(redisClient.sscan(0, 'set1', 3, None, 2))
  print(redisClient.sscan(0, 'set1', 0, None, 3))

  redisClient.srem(0, 'set1', 'value1')
  print(redisClient.sscan(0, 'set1', 0, None, 3))

  redisClient.srem(0, 'set1', 'value2')
  print(redisClient.sscan(0, 'set1', 0, None, 3))

  redisClient.srem(0, 'set1', 'value3')
  print(redisClient.sscan(0, 'set1', 0, None, 3))

  # hashmap test
  redisClient.unlink(0, 'map1')
  redisClient.hset(0, 'map1', 'key1', 'value1')
  redisClient.hset(0, 'map1', 'key2', 'value2')
  redisClient.hset(0, 'map1', 'key3', 'value3')

  print(redisClient.hscan(0, 'map1', 0, 'key*', 2))
  print(redisClient.hscan(0, 'map1', 3, None, 2))
  print(redisClient.hscan(0, 'map1', 0, None, 3))

  # mapResult = redisClient.hscan(0, 'map1', 0, None, 3)
  # keyList = []
  # if len(mapResult[1]) > 0:
  #   for (key, value) in mapResult[1].items():
  #     keyList.append(key)
  #
  # print(keyList)

  redisClient.hdel(0, 'map1', 'key1')
  print(redisClient.hscan(0, 'map1', 0, None, 3))

  redisClient.hdel(0, 'map1', 'key2')
  print(redisClient.hscan(0, 'map1', 0, None, 3))

  redisClient.hdel(0, 'map1', 'key3')
  print(redisClient.hscan(0, 'map1', 0, None, 3))


