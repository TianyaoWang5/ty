# -*- coding:utf-8 -*-

"""
工具包

Author: xunfeng
Date:   2018/04/28
Update: 2018/09/07 1. 增加函数datetime_to_timestamp;
"""

import uuid
import time
import decimal
import datetime
import requests
import json
import asyncio
import psutil
import os
from datetime import timedelta
from datetime import timezone



def get_cur_timestamp():
    """ 获取当前时间戳
    """
    ts = int(time.time())
    return ts


def get_cur_timestamp_ms():
    """ 获取当前时间戳(毫秒)
    """
    ts = int(time.time() * 1000)
    return ts


def get_cur_datetime_m(fmt='%Y%m%d%H%M%S%f'):
    """ 获取当前日期时间字符串，包含 年 + 月 + 日 + 时 + 分 + 秒 + 微妙
    """
    today = datetime.datetime.today()
    str_m = today.strftime(fmt)
    return str_m


def get_datetime(fmt='%Y%m%d%H%M%S'):
    """ 获取日期时间字符串，包含 年 + 月 + 日 + 时 + 分 + 秒
    """
    today = datetime.datetime.today()
    str_dt = today.strftime(fmt)
    return str_dt


def get_date(fmt='%Y%m%d', delta_day=0):
    """ 获取日期字符串，包含 年 + 月 + 日
    @param fmt 返回的日期格式
    """
    day = datetime.datetime.today()
    if delta_day:
        day += datetime.timedelta(days=delta_day)
    str_d = day.strftime(fmt)
    return str_d

def get_beijing_time():
    utc_now = datetime.datetime.utcnow().replace(tzinfo=timezone.utc)
    SHA_TZ = timezone(
        timedelta(hours=8),
        name='Asia/Shanghai',
    )
    # 北京时间
    beijing_now = utc_now.astimezone(SHA_TZ)
    fmt = '%Y-%m-%d %H:%M:%S'
    now_fmt =beijing_now.strftime(fmt)
    return now_fmt


def date_str_to_dt(date_str=None, fmt='%Y%m%d', delta_day=0):
    """ 日期字符串转换到datetime对象
    @param date_str 日期字符串
    @param fmt 日期字符串格式
    @param delta_day 相对天数，<0减相对天数，>0加相对天数
    """
    if not date_str:
        dt = datetime.datetime.today()
    else:
        dt = datetime.datetime.strptime(date_str, fmt)
    if delta_day:
        dt += datetime.timedelta(days=delta_day)
    return dt


def dt_to_date_str(dt=None, fmt='%Y%m%d', delta_day=0):
    """ datetime对象转换到日期字符串
    @param dt datetime对象
    @param fmt 返回的日期字符串格式
    @param delta_day 相对天数，<0减相对天数，>0加相对天数
    """
    if not dt:
        dt = datetime.datetime.today()
    if delta_day:
        dt += datetime.timedelta(days=delta_day)
    str_d = dt.strftime(fmt)
    return str_d


def get_utc_time():
    """ 获取当前utc时间
    """
    utc_t = datetime.datetime.utcnow()
    return utc_t


def ts_to_datetime_str(ts=None, fmt='%Y-%m-%d %H:%M:%S'):
    """ 将时间戳转换为日期时间格式，年-月-日 时:分:秒
    @param ts 时间戳，默认None即为当前时间戳
    @param fmt 返回的日期字符串格式
    """
    if not ts:
        ts = get_cur_timestamp()
    dt = datetime.datetime.fromtimestamp(int(ts))
    return dt.strftime(fmt)


def datetime_str_to_ts(dt_str, fmt='%Y-%m-%d %H:%M:%S'):
    """ 将日期时间格式字符串转换成时间戳
    @param dt_str 日期时间字符串
    @param fmt 日期时间字符串格式
    """
    ts = int(time.mktime(datetime.datetime.strptime(dt_str, fmt).timetuple()))
    return ts


def datetime_to_timestamp(dt=None, tzinfo=None):
    """ 将datetime对象转换成时间戳
    @param dt datetime对象，如果为None，默认使用当前UTC时间
    @param tzinfo 时区对象，如果为None，默认使用timezone.utc
    @return ts 时间戳(秒)
    """
    if not dt:
        dt = get_utc_time()
    if not tzinfo:
        tzinfo = datetime.timezone.utc
    ts = int(dt.replace(tzinfo=tzinfo).timestamp())
    return ts


def utctime_str_to_ts(utctime_str, fmt="%Y-%m-%dT%H:%M:%S.%fZ"):
    """ 将UTC日期时间格式字符串转换成时间戳
    @param utctime_str 日期时间字符串 eg: 2019-03-04T09:14:27.806Z
    @param fmt 日期时间字符串格式
    @return timestamp 时间戳(秒)
    """
    dt = datetime.datetime.strptime(utctime_str, fmt)
    timestamp = int(dt.replace(tzinfo=datetime.timezone.utc).astimezone(tz=None).timestamp())
    return timestamp


def utctime_str_to_mts(utctime_str, fmt="%Y-%m-%dT%H:%M:%S.%fZ"):
    """ 将UTC日期时间格式字符串转换成时间戳（毫秒）
    @param utctime_str 日期时间字符串 eg: 2019-03-04T09:14:27.806Z
    @param fmt 日期时间字符串格式
    @return timestamp 时间戳(毫秒)
    """
    dt = datetime.datetime.strptime(utctime_str, fmt)
    timestamp = int(dt.replace(tzinfo=datetime.timezone.utc).astimezone(tz=None).timestamp() * 1000)
    return timestamp


def get_uuid1():
    """ make a UUID based on the host ID and current time
    """
    s = uuid.uuid1()
    return str(s)


def get_uuid3(str_in):
    """ make a UUID using an MD5 hash of a namespace UUID and a name
    @param str_in 输入字符串
    """
    s = uuid.uuid3(uuid.NAMESPACE_DNS, str_in)
    return str(s)


def get_uuid4():
    """ make a random UUID
    """
    s = uuid.uuid4()
    return str(s)


def get_uuid5(str_in):
    """ make a UUID using a SHA-1 hash of a namespace UUID and a name
    @param str_in 输入字符串
    """
    s = uuid.uuid5(uuid.NAMESPACE_DNS, str_in)
    return str(s)


def float_to_str(f, p=20):
    """ Convert the given float to a string, without resorting to scientific notation.
    @param f 浮点数参数
    @param p 精读
    将浮点数转换为字符串并截取小数点后指定位数,计算速度略微慢,测试10000次耗时约30毫秒
    """
    if type(f) == str:
        f = float(f)
    p0 = 0
    num = 0
    if abs(f)>=1:
        n = abs(f)
        p0 = len(str(n).split('.')[0])
    else:
        n = 1+abs(f)
        for b in str(n).split('.')[1]:
            if int(b) == 0:
                num=num+1
            else:
                break
    ctx = decimal.Context(p+p0-num,rounding="ROUND_DOWN")
    d1 = ctx.create_decimal(repr(f))
    return format(d1, 'f')

def C_N(num, Precision):
    """ 小数保留指定位数
    @param f 浮点数
    @param p 精读
    """
    ret = int(num*10**(Precision+1)/10) / 10**Precision
    return ret

def GetUSDCNY():
    USDCNY = 7
    try:
        trynum = 0
        while trynum<3:
            t = time.time()
            # 这里必须是ms级别的时间戳
            timestamp = (int(round(t*1000)))
            url = "http://www.chinamoney.com.cn/r/cms/www/chinamoney/data/fx/ccpr.json?t="+str(timestamp)
            r = requests.post(url)
            sjson = json.loads(r.text)
            for k in sjson['records']:
                if k['vrtEName'] == 'USD/CNY':
                    USDCNY = round(float(k['price']),3)
                    break
            trynum+=1
    except:
        pass
    return USDCNY

#执行并发任务
async def gotasks(tasks0,*args, **kwargs):
    tasks = []
    for ta in tasks0:
        if ta:
            tasks.append(asyncio.create_task(ta()))
    await asyncio.wait(tasks)
async def asynGo(*args, **kwargs):
    tasks = []
    for ta in args:
        if ta:
            tasks.append(ta)
    asyncio.run(gotasks(tasks))


def getmemoryinfo(): 
    """ 
    获取内存信息
    """
    used = 0
    percent = 0 
    total = 0
    pidmemory = 0
    pid = 0
    try:
        info = psutil.virtual_memory()
        percent = info.percent #内存使用占比
        used = info.used/1024/1024 #MB
        total = info.total/1024/1024 #MB
        #查看当前进程内存占用情况
        pid = os.getpid() #当前进程id
        pidmemory = round(psutil.Process(pid).memory_info().rss / 1024 / 1024,3) #MB     
    except BaseException as e:
        pass
    return pid,pidmemory,used,percent,total

def cut_list(lists, cut_len):
    """
    将列表拆分为指定长度的多个列表
    :param lists: 初始列表
    :param cut_len: 每个列表的长度
    :return: 一个二维数组 [[x,x],[x,x]]
    """
    res_data = []
    if len(lists) > cut_len:
        for i in range(int(len(lists) / cut_len)):
            cut_a = lists[cut_len * i:cut_len * (i + 1)]
            res_data.append(cut_a)

        last_data = lists[int(len(lists) / cut_len) * cut_len:]
        if last_data:
            res_data.append(last_data)
    else:
        res_data.append(lists)

    return res_data
#毫秒
#asyncio.sleep(1000)
async def Sleep(stime):
    await asyncio.sleep(stime/1000)