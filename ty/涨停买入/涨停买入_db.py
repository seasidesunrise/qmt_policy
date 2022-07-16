# encoding:gbk


"""
 触涨停买入，炸板后触涨停买入


 撤单：
    1、封单总量小于1000万，撤单
    2、

:Author:  逍遥游
:Create:  2022/7/15$ 21:37$
"""

import bsea_utils.bsea_xyy_qmt_util as qu
from bsea_utils.bsea_xyy_util import *

策略名称 = '涨停买入'
table_t = 'bsea_涨停买入'

############  人工指定部分开始 ###############

g_策略总金额 = 10 * 10000  # 该策略最大使用的资金量，单位：元
g_单支股票最大使用金额 = 5 * 10000  # 单支股票做大仓位，单位：元

############  人工指定部分结束 ###############


g_countdown_latch = 8
g_prepare_df = pd.DataFrame(columns=['qmt_code', 'name', '跌停价', '涨停价', 'pre_close', '启用炸板回封买入', '板上卖单小于多少万', '启用板上卖单小于多少万选项'])
g_final_df = pd.DataFrame(columns=['qmt_code', 'name'])
g_今天下过的单_set = set()


def recheck_prepare_stocks(ContextInfo):
    curr_time = get_curr_time()
    print(f'------$$$$$$ {策略名称} timerHandler计时器' + get_curr_date() + " " + curr_time)

    global g_prepare_df

    if not check_is_盘中_or_临近(curr_time):
        # todo: 清理 g_prepare_df 中的所有数据
        print(f"{策略名称} 当前时间不在交易中： {curr_time}")
        return

    sql_all_标的 = "SELECT * FROM " + table_t + " WHERE status='1' AND account_nick='" + str(cst.account_nick) + "'"
    all_df = get_df_from_table(sql_all_标的)
    if len(all_df) == 0:
        print(f"{策略名称} 有效标的为空，跳过")

    df_all = ContextInfo.get_market_data(fields=['volume', 'amount', 'open', 'high', 'low', 'close'], stock_code=all_df['qmt_code'].tolist(), period='1d', dividend_type='front', count=2)
    print(df_all)
    for index, row in all_df.iterrows():
        qmt_code = row['qmt_code']
        板上卖单小于多少万 = get_num_by_numfield(row, '板上卖单小于多少万')
        启用板上卖单小于多少万选项 = get_num_by_numfield(row, '启用板上卖单小于多少万选项')
        启用炸板回封买入 = get_num_by_numfield(row, '启用炸板回封买入')
        name = qu.get_name_by_qmtcode(ContextInfo, qmt_code)
        涨停价, 跌停价 = qu.get_涨停_跌停价_by_qmt(ContextInfo, qmt_code)

        df = df_all[qmt_code].copy()
        pre1k_data = df.iloc[-2]
        pre_close = pre1k_data['close']  # 设置pre1k收盘价

        print(f"{策略名称} {qmt_code}[{name}], 跌停价: {fmt_float2str(跌停价)}, 涨停价: {fmt_float2str(涨停价)}")
        g_prepare_df.loc[qmt_code] = {'qmt_code': qmt_code, 'name': name, '跌停价': 跌停价, '涨停价': 涨停价, 'pre_close': pre_close, '启用板上卖单小于多少万选项': 启用板上卖单小于多少万选项, '板上卖单小于多少万': 板上卖单小于多少万}

    print(f"{策略名称}，预备股池: {g_prepare_df}")


def init(ContextInfo):
    log_and_send_im(f"------$$$$$$ {get_curr_date()} {get_curr_time()} {策略名称} 策略已启动init")
    pass_qmt_funcs()
    ContextInfo.set_account(cst.account)

    timer_startTime = get_curr_date() + "06:05:10"
    ContextInfo.run_time("recheck_prepare_stocks", "10nSecond", timer_startTime)


def handlebar(ContextInfo):
    print('{策略名称} 这是 handlebar 中的 3秒一次的tick ~~~')

    global g_prepare_df
    global g_final_df
    global g_今天下过的单_set
    d = ContextInfo.barpos
    realtime = ContextInfo.get_bar_timetag(d)
    nowdate = timetag_to_datetime(realtime, '%Y-%m-%d %H:%M:%S')

    global g_countdown_latch
    g_countdown_latch -= 1
    if g_countdown_latch <= 0:
        g_countdown_latch = 8
        可用资金, 持仓df, obj_list = qu.get_stock_持仓列表(cst.account)

    if len(g_prepare_df) == 0:
        return

    df3 = ContextInfo.get_full_tick(stock_code=g_prepare_df['qmt_code'].tolist())
    if len(g_final_df) > 0:
        print("======== g_final_df ====== ")
        print(g_final_df)

    for index2, row2 in g_prepare_df.iterrows():
        qmt_code = row2['qmt_code']
        code = qmt_code[:6]
        pre_close = row2['pre_close']
        name = row2['name']
        跌停价 = row2['跌停价']
        涨停价 = row2['涨停价']
        板上卖单小于多少万 = row2('板上卖单小于多少万')
        启用板上卖单小于多少万选项 = row2('启用板上卖单小于多少万选项')
        启用炸板回封买入 = row2('启用炸板回封买入')
        curr_data = df3.get(qmt_code)
        if curr_data is None:
            print(f"get data err: {qmt_code}[{name}]")
            continue

        卖价五档 = curr_data['askPrice']
        卖一价格 = 卖价五档[0]
        买价五档 = curr_data['bidPrice']
        买一价格 = 买价五档[0]
        卖量五档 = curr_data['askVol']
        卖一数量 = 卖量五档[0]
        买量五档 = curr_data['bidVol']
        买一数量 = 买量五档[0]
        卖一金额 = 卖一价格 * 卖一数量 * 100 / 10000  # 单位：万
        买一金额 = 买一价格 * 买一数量 * 100 / 10000  # 单位：万
        成交额 = curr_data['amount'] / 10000  # 单位：万
        成交量 = curr_data['volume']  # 单位：手

        small_flt = 1 / 10000 / 10000
        close = curr_data['lastPrice']
        close = round(close + small_flt, 2)
        跌停价 = round(跌停价 - small_flt, 2)
        涨停价 = round(涨停价 + small_flt, 2)

        print(f"{qmt_code}[{name}] 当前价: {close}, 涨停价: {涨停价}, 跌停价: {跌停价},  卖一价格: {卖一价格}, 卖一数量: {卖一数量}, 卖一金额: {卖一金额}, 成交额: {成交额}万, 成交量:{成交量}手, close <= 涨停价: {close >= 涨停价}")

        if close >= 涨停价:
            是否已买 = False
            if len(g_final_df) > 0:
                if len(g_final_df[g_final_df['qmt_code'] == qmt_code]) > 0:  # 先删除
                    是否已买 = True
            if not 是否已买:
                买入价格 = close
                买入股数 = int(g_单支股票最大使用金额 / 买入价格 / 100) * 100
                买入股数 = 100  # todo：买入100股

                if 启用炸板回封买入 == 1:
                    df_all = ContextInfo.get_market_data(fields=['volume', 'amount', 'open', 'high', 'low', 'close'], stock_code=all_df['qmt_code'].tolist(), period='1d', dividend_type='front', count=2)
                    print(df_all)

                else:
                    if 启用板上卖单小于多少万选项 == 1:  # check 卖一选项
                        if 卖一金额 < 板上卖单小于多少万:
                            log_and_send_im(f"{qmt_code}[{name}] 触涨停，卖一金额: {卖一金额}, 板上卖单小于 {板上卖单小于多少万}，下单买入")
                            qu.buy_stock(ContextInfo, qmt_code, name, 涨停价, 买入股数, 策略名称)
                            g_final_df.loc[qmt_code] = {'qmt_code': qmt_code, 'name': name}
                    else:
                        log_and_send_im(f"{qmt_code}[{name}] 触涨停，买一金额: {买一金额}万，涨停价下单买入")
                        qu.buy_stock(ContextInfo, qmt_code, name, 涨停价, 买入股数, 策略名称)
                        g_final_df.loc[qmt_code] = {'qmt_code': qmt_code, 'name': name}

            return


def pass_qmt_funcs():
    qu.passorder = passorder
    qu.get_trade_detail_data = get_trade_detail_data
    qu.get_new_purchase_limit = get_new_purchase_limit
    qu.get_ipo_data = get_ipo_data


def deal_callback(ContextInfo, dealInfo):
    """ 当账号成交状态有变化时，会执行这个函数 """
    qu.deal_callback_func(dealInfo, 策略名称)


def stop(ContextInfo):
    qu.stop_policy(策略名称)
