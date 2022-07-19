# encoding:gbk

"""
定价做T策略

作用：用于建仓完成后高抛低吸

主逻辑：
1、高于某价格price_high卖出;
2、低于某价格price_low买入;
3、跌破某价格price_zs止损卖出;
4、买入、卖出，均采用核按钮下单（大资金遇到小股票谨慎使用）

"""

import bsea_utils.bsea_xyy_qmt_util as qu
from bsea_utils.bsea_xyy_util import *

策略名称 = '定价bs做t'
table_t = 'bsea_做t_定价'


def handlebar(ContextInfo):
    print(f'这是 {策略名称} handlebar 中的 3秒一次的tick ~~~')

    curr_date = get_curr_date()
    curr_time = get_curr_time()
    curr_dtime = curr_date + " " + curr_time

    sql_all_标的 = "SELECT * FROM " + table_t + " WHERE status='1' AND account_nick='" + str(cst.account_nick) + "'"
    all_df = get_df_from_table(sql_all_标的)
    if len(all_df) == 0:
        print(f"{curr_dtime} {策略名称} 有效标的为空，跳过")
        return

    for index, row in all_df.iterrows():
        qmt_code = row['qmt_code']
        name = qu.get_name_by_qmtcode(ContextInfo, qmt_code)

        price_high = get_num_by_numfield(row, 'price_high')
        price_low = get_num_by_numfield(row, 'price_low')
        price_zs = get_num_by_numfield(row, 'price_zs')
        做t资金 = get_num_by_numfield(row, '做t资金')  # 当前做t支配的资金量，做T资金
        rt_当前持股数 = get_num_by_numfield(row, 'rt_当前持股数')
        rt_当前做t状态 = get_str_by_strfield(row, 'rt_当前做t状态')

        df = ContextInfo.get_market_data(fields=['volume', 'close'], stock_code=[qmt_code], period='1d', dividend_type='front', count=1)
        if len(df) == 0 or df.iloc[0]['volume'] == 0:  # 判断volume是为了过滤停牌
            log_and_send_im_with_ttl(f"{curr_dtime} {策略名称} {qmt_code}[{name}] 获取行情数据失败，跳过")
            continue
        curr_data = df.iloc[0]
        当前价格 = curr_data['close']

        if 当前价格 < price_zs:  # 跌破止损均线，止损
            卖出数量 = qu.get_可卖股数_by_qmtcode(cst.account, qmt_code)
            if 卖出数量 == 0:
                log_and_send_im_with_ttl(f"{curr_dtime} {策略名称} {qmt_code}[{name}] 达到止损卖出条件，但卖出股数为 0")
                continue

            卖出数量 = 100  # todo：应该全部卖掉
            qu.sell_stock_he(ContextInfo, qmt_code, name, 卖出数量, 策略名称)  # 带价格笼子的核按钮卖出

            save_or_update_by_sql("UPDATE " + table_t + " SET status='0', lastmodified='" + get_lastmodified() + "' WHERE qmt_code='" + qmt_code + "'")
            log_and_send_im(f"{curr_dtime} {策略名称} {qmt_code}[{name}] 卖出数量: {卖出数量} 达到止损卖出条件，已清仓！！")
        else:
            where_clause = " WHERE qmt_code='" + qmt_code + "' AND account_nick='" + cst.account_nick + "'"
            if (rt_当前做t状态 == '' or rt_当前做t状态 == '已T出') and (price_zs < 当前价格 <= price_low):  # 刚开始，或已经卖； 当价格跌到买入价位置，执行'买回'动作
                买入股数 = int(做t资金 / 当前价格 / 100) * 100
                if 买入股数 < 100:
                    log_and_send_im_with_ttl(f"{curr_dtime} {策略名称} {qmt_code}[{name}] 买入股数 不足100股，做t资金：{做t资金}")
                    continue
                买入股数 = 100  # todo: 仓位大小需要

                qu.buy_stock_he(ContextInfo, qmt_code, name, 买入股数, 策略名称)

                t_status = T_Type.已买回.value
                update_sql = "UPDATE " + table_t + " SET rt_当前做t状态='" + t_status + "', rt_当前持股数='" + str(买入股数) + "', lastmodified='" + get_lastmodified() + "'" + where_clause
                save_or_update_by_sql(update_sql)
                continue

            if (rt_当前做t状态 == '' or rt_当前做t状态 == '已买回') and (当前价格 >= price_high):  # 刚开始，或已经买； 当价格上升到卖出价位置，执行'卖出'动作
                可卖股数 = qu.get_可卖股数_by_qmtcode(cst.account, qmt_code)
                做t资金卖出股数 = int(做t资金 / 当前价格 / 100) * 100
                卖出股数 = min(rt_当前持股数, 做t资金卖出股数, 可卖股数)  # 取db中的当前持股数与持仓中的可卖股数，取数字小的那个卖出
                if 卖出股数 < 100:
                    log_and_send_im_with_ttl(f"{curr_dtime} {策略名称} {qmt_code}[{name}] 达到卖出条件，但卖出股数为零。db卖出股数：{rt_当前持股数}, 持仓可卖股数: {可卖股数}")
                    continue
                卖出股数 = 100  # todo: 仓位大小需要

                qu.sell_stock_he(ContextInfo, qmt_code, name, 卖出股数, 策略名称)

                t_status = T_Type.已t出.value
                update_sql = "UPDATE " + table_t + " SET rt_当前做t状态='" + t_status + "', rt_当前持股数='" + str(0) + "', lastmodified='" + get_lastmodified() + "'" + where_clause
                save_or_update_by_sql(update_sql)
                continue


def init(ContextInfo):
    log_and_send_im(f"------$$$$$$ {get_curr_date()}  {get_curr_time()}  {策略名称} 策略已启动init")
    pass_qmt_funcs()
    ContextInfo.set_account(cst.account)


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
