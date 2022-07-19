"""
 
:Author:  逍遥游
:Create:  2022/6/28$ 16:44$
"""

import json
import sys
import time
from datetime import date, timedelta
from enum import Enum

import pandas as pd
import requests
from pandas import NaT

import bsea_cst as cst
from cacheout import Cache

cache = Cache(maxsize=10000, ttl=0, timer=time.time, default=None)  # defaults

class 定价条件单(Enum):
    定价买入 = '定价买入'
    定价卖出 = '定价卖出'


class T_Type(Enum):
    已t出 = '已t出'
    已买回 = '已买回'
    其它 = ''


def log_and_send_im(text):
    # 定义要发送的数据
    data = {
        "msg_type": "text",
        "content": {"text": text + '\n'}
    }
    # 发送post请求
    try:
        print(text)
        requests.post(cst.webhook, data=json.dumps(data), headers=cst.headers)
    except:
        print("Unexpected error:", sys.exc_info()[0])


def log_and_send_im_with_ttl(text, ttl=600):
    """ ttl为秒数 """
    if cache.get(text) is not None:
        print(f"消息文本命中cache，防重发生效，跳过: {text}")
        return
    log_and_send_im(text)
    cache.set(text, text, ttl=ttl)


def get_df_from_table(select_sql):
    print(select_sql)
    return pd.read_sql(select_sql, cst.engine)


def save_or_update_by_sql(update_sql):
    print(update_sql)
    cst.session.execute(update_sql)
    cst.session.commit()


def fmt_float2str(number):
    return str(format(float(number), "0.2f"))


def fmt_float2pct(value, reserve_count=2):
    return str(format(float(value) * 100, "0." + str(reserve_count) + "f") + "%")


def get_curr_date():
    return time.strftime("%Y-%m-%d", time.localtime())


def get_curr_time():
    return time.strftime("%H:%M:%S", time.localtime())


def get_lastmodified():
    """ 返回当前字符串格式的时间，作为lastmodified写入到db """
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def get_zuotian_date():
    yesterday = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
    print(yesterday)
    return yesterday


def get_涨停_跌停价(code, pre_close_ori):
    涨跌停系数 = 0.1
    if code.startswith('688') or code.startswith('3'):
        涨跌停系数 = 0.2

    # 计算当日涨停、跌停价
    small_flt = 1 / 10000 / 10000
    当日涨停价 = round(pre_close_ori * (1 + 涨跌停系数) + small_flt, 2)
    当日跌停价 = round(pre_close_ori * (1 - 涨跌停系数) + small_flt, 2)
    return 当日涨停价, 当日跌停价


def get_卖出价格(qmt_code, pre_close_ori, 当前价, 滑点=0.02):
    if 滑点 < 0 or 滑点 > 0.2:
        滑点 = 0.02
    当日涨停价, 当日跌停价 = get_涨停_跌停价(qmt_code, pre_close_ori)
    卖出价格 = max((1 - 滑点) * 当前价, 当日跌停价)
    return 卖出价格


def get_number(s):
    try:
        val = float(s)
        return val
    except Exception:
        pass
    return None


def get_num_by_numfield(_row, field, default_value=0):
    """
    从dict中取出对应字段的数字形式，字段不存在返回0
    """
    if field not in _row.keys():
        return default_value
    val = _row[field]
    val2 = get_number(val)
    if val2 is None:
        return default_value
    else:
        return val2


def get_str_by_numfield(_row, field, default_value=0):
    """
    从dict中取出对应字段的字符串形式，字段不存在返回'0'
    """
    return str(get_num_by_numfield(_row, field, default_value))


def get_str_by_strfield(_row, field, default_value=''):
    """
    从dict中取出对应字段的字符串形式，字段不存在返回''
    """
    if field not in _row.keys():
        return default_value
    val = _row[field]
    if val is None:
        return default_value
    else:
        return str(val)


def get_dtime_by_datefield(_row, field, default_value=None):
    if field not in _row.keys():
        return default_value
    val = _row[field]
    if val is None or val == NaT:
        return default_value
    else:
        val_dt = str(val)
        if val_dt is not None and val_dt.strip() != '':
            return str(val_dt)[:10]
        else:
            return None


def get_涨幅(now_price, pre_price):
    if pre_price == 0:
        return 100 * 10000
    else:
        return 100 * (now_price - pre_price) / pre_price


def check_is_盘中_or_临近(curr_time=None):
    """ 开盘和收盘都给了3分钟的余量空档 """
    if curr_time is None:
        curr_time = get_curr_time()

    if (curr_time >= '09:27:00' and curr_time < '11:33:00') or (curr_time >= '12:57:00' and curr_time < '15:03:00'):  # 卖出
        return True
    else:
        return False
