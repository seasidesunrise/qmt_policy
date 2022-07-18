# encoding:gbk

"""
3点收盘后触发

国债逆回购

"""

import bsea_utils.bsea_xyy_qmt_util as qu
from bsea_utils.bsea_xyy_util import *

策略名称 = '自动国债逆回购'


def timerHandler(ContextInfo):
    curr_time = get_curr_time()
    curr_date = get_curr_date()

    print(f'------$$$$$$ {策略名称} timerHandler计时器 {curr_date} {curr_time}')

    if (curr_time >= '15:03:00' and curr_time < '15:33:00'):  # 全量资金all in国债逆回购1天期, 有防重复下单功能，实际钱不够也不可能重复买入。逆回购交易时间延长到15:30
        qu.国债逆回购(ContextInfo, cst.account)
        print(f"{策略名称} 已收盘, sleep 30s")
        time.sleep(30)

    if curr_time >= '15:33:00':  # 已收盘
        print(f"{策略名称} 已收盘, sleep 300s")
        time.sleep(600)


def init(ContextInfo):
    log_and_send_im(f"------$$$$$$ {get_curr_date()} {get_curr_time()} {策略名称} 已启动init")
    pass_qmt_funcs()

    ContextInfo.set_account(cst.account)
    timer_startTime = get_curr_date() + "09:20:10"

    ContextInfo.run_time("timerHandler", "3nSecond", timer_startTime)


def handlebar(ContextInfo):
    print(f'{策略名称} 这是 handlebar 中的 say hi~~~')


def pass_qmt_funcs():
    qu.passorder = passorder
    qu.get_trade_detail_data = get_trade_detail_data
    qu.get_new_purchase_limit = get_new_purchase_limit
    qu.get_ipo_data = get_ipo_data
    qu.cancel = cancel


def stop(ContextInfo):
    qu.stop_policy(策略名称)


def deal_callback(ContextInfo, dealInfo):
    """ 当账号成交状态有变化时，会执行这个函数 """
    qu.deal_callback_func(dealInfo, 策略名称)
