# -*- coding: utf-8 -*-
#!/usr/bin/env python

'''
时间日期工具类
'''

import datetime

class DateUtil(object):
  '''
  时间日期工具类
  '''

  def __init__(self):
    pass

  @staticmethod
  def format4Y2m2d(formatDate):
    '''
    格式化时间
    :param formatDate: 待格式化的时间
    :return:
    '''
    return formatDate.strftime('%Y%m%d')

  @staticmethod
  def format4Y2m2d2H(formatDate):
    '''
    格式化时间
    :param formatDate: 待格式化的时间
    :return:
    '''
    return formatDate.strftime('%Y%m%d%H')

  @staticmethod
  def addDaysOnCurrent(amount):
    '''
    在当前时间上增加amount天，amount可为+/-
    :param amount: amount 天
    :return:
    '''
    now = datetime.datetime.now()
    delta = datetime.timedelta(days=amount)
    newDate = now + delta
    return newDate

  @staticmethod
  def addHoursOnCurrent(amount):
    '''
    在当前时间上增加amount小时，amount可为+/-
    :param amount: amount 小时
    :return:
    '''
    now = datetime.datetime.now()
    delta = datetime.timedelta(hours=amount)
    newDate = now + delta
    return newDate

  @staticmethod
  def getCurrentDate():
    return datetime.datetime.now()

  @staticmethod
  def splitIntoDays(fromDate, toDate):
    '''
    以天为单位分割from-to之间的时间
    :param fromDate: 开始时间
    :param toDate: 结束时间
    :return: 日期list
    '''
    if None == fromDate or None == toDate or toDate < fromDate:
      return []

    dayStrList = []
    tmpDate = fromDate
    while tmpDate < toDate:
      dayStrList.append(DateUtil.format4Y2m2d(tmpDate))
      tmpDate = tmpDate + datetime.timedelta(days=1)

    return dayStrList

  @staticmethod
  def splitIntoHours(fromDate, toDate):
    '''
    以小时为单位分割from-to之间的时间
    :param fromDate: 开始时间
    :param toDate: 结束时间
    :return: 日期list
    '''
    if None == fromDate or None == toDate or toDate < fromDate:
      return []

    dayStrList = []
    tmpDate = fromDate
    while tmpDate < toDate:
      dayStrList.append(DateUtil.format4Y2m2d2H(tmpDate))
      tmpDate = tmpDate + datetime.timedelta(hours=1)

    return dayStrList

if __name__ == '__main__':
  validDate = 3;
  print(DateUtil.format4Y2m2d(DateUtil.addDaysOnCurrent(-validDate)))

  fromDate = DateUtil.addDaysOnCurrent(-3)
  toDate = DateUtil.getCurrentDate()

  dayStrList = DateUtil.splitIntoDays(fromDate, toDate)

  print(dayStrList)

  fromDate = DateUtil.addHoursOnCurrent(-7)
  toDate = DateUtil.getCurrentDate()

  hourStrList = DateUtil.splitIntoHours(fromDate, toDate)
  print(hourStrList.__str__())
