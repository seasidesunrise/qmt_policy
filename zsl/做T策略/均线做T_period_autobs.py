# encoding:gbk

"""
均线做T策略: 高于某根均线5%卖出，低于均线5%买入

主逻辑：
1、高于30线5个点卖出;
2、低于30线5个点买入;
3、跌破60均线止损全部卖出;
4、兼容各种period：'5m', '15m', '30m', '1h', '1d', '1w', '1mon'
5、买入、卖出，均采用核按钮下单（大资金遇到小股票谨慎使用）

todo：
1、止损时，是否要先做撤单；

"""
import talib

import bsea_utils.bsea_xyy_qmt_util as qu
from bsea_utils.bsea_xyy_util import *

策略名称 = '均线做T'  # 先卖后买
table_t = "bsea_做t_均线_period"

g_data = {}


def handlebar(ContextInfo):
    print(f'{策略名称} 这是 handlebar 中的 3秒一次的tick ~~~')

    sql_all_标的 = "SELECT * FROM " + table_t + " WHERE status='1' AND account_nick='" + str(cst.account_nick) + "'"
    all_df = get_df_from_table(sql_all_标的)
    if len(all_df) == 0:
        print(f"{策略名称} 有效标的为空，跳过")

    global g_data
    for index, row in all_df.iterrows():
        qmt_code = row['qmt_code']
        name = qu.get_name_by_qmtcode(ContextInfo, qmt_code)

        做t均线 = get_num_by_numfield(row, '做t均线')
        做t止损均线 = get_num_by_numfield(row, '做t止损均线')
        高于均线百分比卖出 = get_num_by_numfield(row, '高于均线百分比卖出')  # 如5，即表示高于均线5%卖出
        低于均线百分比买入 = get_num_by_numfield(row, '低于均线百分比买入')  # 如5，即表示低于均线5%买入
        初始做t资金 = get_num_by_numfield(row, '初始做t资金')  # 当前做t支配的资金量
        rt_当前做t状态 = get_str_by_strfield(row, 'rt_当前做t状态')
        period = get_str_by_strfield(row, 'period')  # 周期
        if period is None or period not in qu.period_list:
            log_and_send_im(f"{策略名称} {qmt_code}[{name}] period 设置错误，必须为：{qu.period_list} 其中之一，请检查，此条做T策略忽略！！")
            continue
        if 做t均线 <= 1:
            log_and_send_im(f"{策略名称} {qmt_code}[{name}] 均线设置错误， 做t均线：{做t均线}，请检查，此条做T策略忽略！！")
            continue
        if 做t止损均线 <= 1:
            log_and_send_im(f"{策略名称} {qmt_code}[{name}] 均线设置错误， 止损均线：{做t止损均线}，请检查，此条做T策略忽略！！")
            continue

        df = get_quatation_by_params(ContextInfo, qmt_code, period, 做t均线, 做t止损均线)
        curr_data = df.iloc[-1]

        当前价格 = curr_data['close']
        where_clause = " WHERE qmt_code='" + qmt_code + "' AND account_nick='" + cst.account_nick + "'"

        if 做t止损均线 < 1000 and curr_data['pre_close'] < curr_data['ma' + str(做t止损均线)]:  # 止损
            卖出数量 = qu.get_可卖股数_by_qmtcode(cst.account, qmt_code)
            if 卖出数量 == 0:
                key = qmt_code + "_" + get_curr_date() + "_zs"
                if g_data.get(key) is None:
                    g_data.update({key: '1'})
                    log_and_send_im(f"{策略名称} {qmt_code}[{name}] 达到止损卖出条件，但卖出股数为 0")
                continue

            卖出数量 = 100  # todo：应该全部卖掉
            qu.he_sell_stock(ContextInfo, qmt_code, name, 卖出数量, 策略名称)  # 核按钮卖

            save_or_update_by_sql("UPDATE " + table_t + " SET status='0' " + where_clause)
            log_and_send_im(f"{策略名称} {qmt_code}[{name}] 达到止损卖出条件，已下单清仓！！")
        else:
            相比均线涨幅 = curr_data['相比均线涨幅']
            # 检查偏离均线幅度
            if (相比均线涨幅 >= 高于均线百分比卖出) and (rt_当前做t状态 == '' or rt_当前做t状态 == '已买回'):  # 做T动作：卖出
                持仓可卖股数 = qu.get_可卖股数_by_qmtcode(qmt_code)
                做t卖出股数 = int(初始做t资金 / 当前价格 / 100) * 100
                卖出股数 = min(做t卖出股数, 持仓可卖股数)  # 取db中的当前持股数与持仓中的可卖股数，取数字小的那个卖出， todo：当前持股数逻辑需要讨论修改，测试期间先忽略
                if 卖出股数 == 0:
                    print(f"{策略名称} {qmt_code}[{name}] 达到卖出条件，但卖出股数为零。做t卖出股数：{做t卖出股数}, 持仓可卖股数: {持仓可卖股数}")
                    continue
                卖出股数 = 100  # todo: 仓位，测试期间暂定100股

                qu.he_sell_stock(ContextInfo, qmt_code, name, 卖出股数, 策略名称)  # 核按钮卖

                t_status = T_Type.已t出.value
                update_sql = "UPDATE " + table_t + " SET 当前做t状态='" + t_status + "', 当前持股数='" + str(0) + "' " + where_clause
                save_or_update_by_sql(update_sql)
                continue

        t出全部成交 = qu.check_委托是否已全部成交(qmt_code)
        if t出全部成交 and (相比均线涨幅 <= -低于均线百分比买入 < 0) and (rt_当前做t状态 == '' or rt_当前做t状态 == '已T出'):  # 做T动作：买回
            买入股数 = int(初始做t资金 / 当前价格 / 100) * 100
            if 买入股数 < 100:
                print(f"{策略名称} {qmt_code}[{name}] 达到买入条件，但可买入股数不足一手。买入股数：{买入股数}, 做t资金: {初始做t资金}")
                continue
            买入股数 = 100  # todo: 仓位大小需要

            qu.he_buy_stock(ContextInfo, qmt_code, name, 买入股数, 策略名称)  # 核按钮买

            t_status = T_Type.已买回.value
            update_sql = "UPDATE " + table_t + " SET 当前做t状态='" + t_status + "', 当前持股数='" + str(买入股数) + "' " + where_clause
            save_or_update_by_sql(update_sql)
            continue

    d = ContextInfo.barpos
    realtime = ContextInfo.get_bar_timetag(d)
    nowdate = timetag_to_datetime(realtime, '%Y-%m-%d %H:%M:%S')
    print(nowdate)


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


def get_quatation_by_params(ContextInfo, qmt_code, period, 做t均线, 止损均线=None):
    cnt = 做t均线 if 止损均线 is None else max(做t均线, 止损均线)
    df = ContextInfo.get_market_data(['volume', 'amount', 'open', 'high', 'low', 'close'], stock_code=[qmt_code], period=period, dividend_type='front', count=int(cnt + 10))
    ma_colname = 'ma' + str(做t均线)
    df[ma_colname] = talib.MA(df['close'], 做t均线)
    if 止损均线 is not None:
        df['ma' + str(止损均线)] = talib.MA(df['close'], 止损均线)
    df['pre_close'] = df['close'].shift(1)
    df['涨幅'] = 100 * (df['close'] - df['pre_close']) / df['pre_close']
    df['相比均线涨幅'] = 100 * (df['close'] - df[ma_colname]) / df[ma_colname]
    return df


def deal_callback(ContextInfo, dealInfo):
    """ 当账号成交状态有变化时，会执行这个函数 """
    qu.deal_callback_func(dealInfo, 策略名称)


def stop(ContextInfo):
    qu.stop_policy(策略名称)
