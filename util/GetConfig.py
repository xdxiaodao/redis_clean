# -*- coding: utf-8 -*-
# !/usr/bin/env python
"""
fetch config from config.ini
"""

import os
from ConfigParser import ConfigParser  # py2


class GetConfig(object):
  """
  to get config from config.ini
  """

  def __init__(self):
    self.pwd = os.path.split(os.path.realpath(__file__))[0]
    self.config_path = os.path.join(os.path.split(self.pwd)[0], 'config.ini')
    self.config_file = ConfigParser()
    self.config_file.read(self.config_path)

  def redis_host(self):
    return self.config_file.get('Redis', 'ads.redis.host')

  def redis_port(self):
    return self.config_file.get('Redis', 'ads.redis.port')

  def redis_password(self):
    return self.config_file.get('Redis', 'ads.redis.password')

  def redis_timeout(self):
    return self.config_file.get('Redis', 'ads.redis.timeout')

if __name__ == '__main__':
  gg = GetConfig()
  print(gg.redis_host())
  print(gg.redis_password())
  print(gg.redis_port())
  print(gg.redis_timeout())