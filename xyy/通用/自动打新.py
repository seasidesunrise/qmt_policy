# encoding:gbk

"""
10�����Ҵ����������¹ɡ���ծ

�Զ�����

"""

import bsea_utils.bsea_xyy_qmt_util as qu
from bsea_utils.bsea_xyy_util import *

�������� = '�Զ�����'


def timerHandler(ContextInfo):
    curr_time = get_curr_time()
    curr_date = get_curr_date()

    print(f'------$$$$$$ {��������} timerHandler��ʱ�� {curr_date} {curr_time}')

    if (curr_time >= '09:59:40' and curr_time < '10:02:00'):  # �¹�_��ծ_�깺
        qu.�¹�_��ծ_�깺(ContextInfo)

    if curr_time >= '15:33:00':  # ������
        print(f"{��������} ������, sleep 300s")
        time.sleep(600)


def init(ContextInfo):
    log_and_send_im(f"------$$$$$$ {��������} ������init")
    pass_qmt_funcs()

    ContextInfo.set_account(cst.account)
    timer_startTime = get_curr_date() + "06:20:10"

    ContextInfo.run_time("timerHandler", "3nSecond", timer_startTime)


def handlebar(ContextInfo):
    print(f'{��������} ���� handlebar �е� say hi~~~')


def pass_qmt_funcs():
    qu.passorder = passorder
    qu.get_trade_detail_data = get_trade_detail_data
    qu.get_new_purchase_limit = get_new_purchase_limit
    qu.get_ipo_data = get_ipo_data
    qu.cancel = cancel


def stop(ContextInfo):
    qu.stop_policy(��������)


def deal_callback(ContextInfo, dealInfo):
    """ ���˺ųɽ�״̬�б仯ʱ����ִ��������� """
    qu.deal_callback_func(dealInfo, ��������)
