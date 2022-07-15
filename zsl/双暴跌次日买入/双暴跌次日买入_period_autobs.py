# encoding:gbk

"""
双暴跌次日买入策略

"""

import talib

import bsea_utils.bsea_xyy_qmt_util as qu
from bsea_utils.bsea_xyy_util import *

策略名称 = '双暴跌买入_period'
buy_table = 'bsea_buy_info'
sell_table = 'bsea_sell_info'

############  人工指定部分开始 ###############

g_code_set = []  # 股票标的，需要建一个'双暴跌买入' 的自定义板块，作为股票池

g_策略总金额 = 10 * 10000  # 该策略最大使用的资金量，单位：元
g_单支股票最大使用金额 = 5 * 10000  # 单支股票做大仓位，单位：元

g_pre1k下跌百分比 = 5  # 填0-20之间的数值，如填5，则表示下跌5%或以上，即下跌幅度>=5%
g_pre2k下跌百分比 = 5  # 填0-20之间的数值，如填5，则表示下跌5%或以上，即下跌幅度>=5%
g_curr下跌阈值 = 1  # 填0-20之间的数值，如填3，则表示下跌3%或以上，就执行买入

g_止盈比例 = 10  # 填0以上的数值，如填10，则表示10%就自动止盈
g_止损比例 = 10  # 填0以上的数值，如填10，则表示10%就自动止损
g_启用CCI向上 = False  # cci
g_启用成交量 = True  # 成交量：curr、pre1k、pre2k，都需要小于ma5成交量
g_period = '1h'  # 设置周期：period支持5m, 15m, 30m, 1h, 1d, 1w, 1mon

############  人工指定部分结束 ###############

g_countdown_latch = 8
g_prepare_df = pd.DataFrame()  # 'code' code, '满足双暴跌买入条件': 1, 'pre_close': 8.5
g_今天下过的单_set = set()
g_sell_委托单_num = 0
g_code_set = set()


def recheck_prepare_stocks(ContextInfo):
    print(f'------$$$$$$ {策略名称} timerHandler计时器' + get_curr_date() + " " + get_curr_time())
    global g_code_set
    global g_prepare_df
    global g_period
    g_prepare_df = pd.DataFrame()
    g_code_set = set()

    s = ContextInfo.get_stock_list_in_sector('双暴跌买入')
    print(f"{策略名称} 双暴跌买入板块成分股：" + str(s))
    g_code_set = set(s)
    print(f"{策略名称} g_code_set: {g_code_set}")
    print(f"{策略名称} g_code_list: {list(g_code_set)}")

    zuotian_date = get_zuotian_date()

    df_all = ContextInfo.get_market_data(fields=['volume', 'amount', 'open', 'high', 'low', 'close'], stock_code=list(g_code_set), period=g_period, dividend_type='front', count=10)
    print(df_all)
    hq_all_dict = {}

    for qmt_code in g_code_set:
        name = qu.get_name_by_qmtcode(ContextInfo, qmt_code)
        cnt_ma5_lower = 3
        df = df_all[qmt_code].copy()
        if len(df) <= cnt_ma5_lower:
            print(f"{策略名称} 取到的k线数据太少，小于等于{cnt_ma5_lower}，忽略")
            continue
        df['ma5'] = talib.MA(df['close'], 5)
        df['pre_close'] = df['close'].shift(1)
        df['涨幅'] = 100 * (df['close'] - df['pre_close']) / df['pre_close']
        df['close低于ma5'] = df.apply(lambda x: 1 if (x['close'] <= x['ma5']) else 0, axis=1)
        print(df)
        hq_all_dict.update({"code": qmt_code, "df": df})

        pre1k_data = df.iloc[-2]
        pre2k_data = df.iloc[-3]
        if pre1k_data['涨幅'] <= -g_pre1k下跌百分比 < 0 and pre2k_data['涨幅'] <= -g_pre2k下跌百分比 < 0:
            满足双暴跌买入预备条件 = True
            pre_close = pre1k_data['close']  # 设置pre1k收盘价
            log_and_send_im(f"{策略名称} {qmt_code}[{name}] 满足双暴跌买入预备条件，开始监控择机买入， pre_close: {fmt_float2str(pre_close)}")
            g_prepare_df = g_prepare_df.append({'qmt_code': qmt_code, '满足双暴跌买入预备条件': 满足双暴跌买入预备条件, 'pre_close': pre_close, 'name': name}, ignore_index=True)

    print(f"{策略名称} 第一、第二天满足条件，预备股池: {g_prepare_df}")


def init(ContextInfo):
    log_and_send_im(f"------$$$$$$ {get_curr_date()} {get_curr_time()} {策略名称} 策略已启动init")
    pass_qmt_funcs()
    ContextInfo.set_account(cst.account)

    timer_startTime = get_curr_date() + "00:05:10"
    ContextInfo.run_time("recheck_prepare_stocks", "20nSecond", timer_startTime)


def handlebar(ContextInfo):
    print('{策略名称} 这是 handlebar 中的 3秒一次的tick ~~~')

    global g_prepare_df
    global g_period
    d = ContextInfo.barpos
    realtime = ContextInfo.get_bar_timetag(d)
    nowdate = timetag_to_datetime(realtime, '%Y-%m-%d %H:%M:%S')

    global g_countdown_latch
    g_countdown_latch -= 1
    if g_countdown_latch <= 0:
        g_countdown_latch = 8
        可用资金, 持仓df, obj_list = qu.get_stock_持仓列表(cst.account)

    print(g_prepare_df)
    if len(g_prepare_df) == 0:
        return

    for index2, row2 in g_prepare_df.iterrows():
        qmt_code = row2['qmt_code']
        code = qmt_code[:6]
        pre_close = row2['pre_close']
        name = row2['name']

        df3 = ContextInfo.get_market_data(fields=['volume', 'amount', 'open', 'high', 'low', 'close'], stock_code=[qmt_code], period=g_period, dividend_type='front', count=1)
        print(df3)
        if len(df3) > 0:
            curr_data = df3.iloc[0]
            close = curr_data['close']
            涨幅 = get_涨幅(close, pre_close)
            if 涨幅 <= -g_curr下跌阈值 < 0:
                # 判断是否已买入
                dtime_curr = get_curr_date()

                select_sql = "SELECT * FROM " + buy_table + " WHERE code='" + qmt_code[:6] + "' AND dtime='" + dtime_curr + "' AND 策略='" + 策略名称 + "' AND status=1"
                select_df = get_df_from_table(select_sql)
                if len(select_df) > 0:  # 已下过单
                    print(f"{策略名称} 已下过单，忽略: {select_df.iloc[0]}")
                    continue

                # 计算CCI
                global g_启用CCI向上
                global g_启用成交量
                if g_启用成交量 or g_启用CCI向上:
                    df32 = ContextInfo.get_market_data(fields=['volume', 'amount', 'open', 'high', 'low', 'close'], stock_code=[qmt_code], period=g_period, dividend_type='front', count=21)
                    if len(df32) == 0:
                        log_and_send_im(f"{策略名称} 获取cci、均量线指标数据源出错, 请联系qmt或检查网络状态")
                        continue

                    if g_启用CCI向上:
                        cci_timeperiode = 14
                        if len(df32) == 0:
                            log_and_send_im(f"{策略名称} 获取cci指标数据源出错, 请联系qmt或检查网络状态")
                            continue
                        df32['cci'] = talib.CCI(df32['high'], df32['low'], df32['close'], cci_timeperiode)
                        print(df32)
                        curr_data_cci = df32.iloc[-1]
                        pre1k_data_cci = df32.iloc[-2]
                        pre2k_data_cci = df32.iloc[-3]
                        pre3k_data_cci = df32.iloc[-4]
                        cci_cond = False
                        if curr_data_cci['cci'] > pre1k_data_cci['cci'] > pre2k_data_cci['cci'] > pre3k_data_cci['cci']:
                            cci_cond = True
                        if not cci_cond:
                            print(f"{策略名称} CCI指标为: {curr_data_cci}, 不满足连续3K一天比一天大，忽略")
                            continue

                    if g_启用成交量:
                        vol_period = 5
                        df32['vol_ma5'] = talib.MA(df32['volume'], vol_period)
                        print(df32)
                        curr_data = df32.iloc[-1]
                        pre1k_data = df32.iloc[-2]
                        pre2k_data = df32.iloc[-3]
                        vol_cond = False
                        if curr_data['volume'] < curr_data['vol_ma5'] and pre1k_data['volume'] < pre1k_data['vol_ma5'] and pre2k_data['volume'] < pre2k_data['vol_ma5']:
                            vol_cond = True
                        if not vol_cond:
                            print(f"{策略名称} vol均量线指标为: {curr_data}, 不满足连续3K低于5均限量，忽略")
                            continue

                可用资金 = qu.get_可用资金()
                买入资金 = min(g_单支股票最大使用金额, 可用资金)
                买入价格 = close  # 买入价设置为当前价
                买入股数 = 100 * int(买入资金 / 买入价格 / 100)
                买入股数 = 100  # todo：测试用，最大买入数量100股

                qu.he_buy_stock(ContextInfo, qmt_code, name, 买入股数, 策略名称)
                log_and_send_im(f"{策略名称} {name}[{qmt_code}]达到第三天下跌阈值{g_curr下跌阈值}%，委托买入，下单金额: {买入资金}, 委托价格：核按钮买入, 买入股数： {买入股数}")

                # insert到已买入表，留作日志用
                买入时间 = dtime_curr + " " + get_curr_time()
                insert_sql_buy = "REPLACE INTO " + buy_table + "(code, qmt_code, dtime, name, status, 策略, 买入价格, 买入股数, 买入金额, 下单时间, period, account_nick, lastmodified) values ('" \
                                 + code + "', '" + qmt_code + "', '" + dtime_curr + "', '" + name + "', 1, '" + 策略名称 + "', '" \
                                 + str(买入价格) + "', '" + str(买入股数) + "', '" + str(买入股数 * 买入价格) + "', '" + dtime_curr + "', '" \
                                 + g_period + "', '" + cst.account_nick + "', '" + get_lastmodified() + "')"
                save_or_update_by_sql(insert_sql_buy)

                # insert到待卖出表
                insert_sql_sell = "REPLACE INTO " + sell_table + "(qmt_code, name, 买入时间, 买入策略名称, 是否卖出, period, account_nick, lastmodified) values ('" + \
                                  code + "', '" + name + "', '" + 买入时间 + "', '" + 策略名称 + "', '0', '" + g_period + "', '" + cst.account_nick + "', '" + get_lastmodified() + "')"
                save_or_update_by_sql(insert_sql_sell)

    # 检查卖出逻辑
    sql = "SELECT * FROM " + sell_table + " WHERE 买入策略名称='" + 策略名称 + "' AND 买入时间<'" + get_curr_date() + "' AND 是否卖出=0 AND account_nick='" + cst.account_nick + "' ORDER BY qmt_code ASC"
    sell_df = get_df_from_table(sql)
    if len(sell_df) == 0:
        print("{策略名称} 暂无卖出标的，忽略")
        return

    可用资金, 持仓df, obj_list = qu.get_stock_持仓列表()
    for index4, row4 in sell_df.iterrows():
        qmt_code = row4['qmt_code']
        持仓df2 = 持仓df[持仓df['qmt_code'] == qmt_code].copy()
        if len(持仓df2) == 0:
            print(f"{策略名称} 持仓中已经无此标的: {qmt_code}")
            update_sql = "UPDATE " + sell_table + " SET 是否卖出=1 WHERE qmt_code='" + qmt_code + "'"
            save_or_update_by_sql(update_sql)
            continue
        data0 = 持仓df2.iloc[0]
        if data0['盈亏比例'] >= g_止盈比例 / 100:
            # 下单卖出
            卖出价格 = data0['当前价']
            卖出数量 = data0['可卖数量']
            当前持仓量 = data0['当前持仓量']
            if 卖出数量 <= 0:
                log_and_send_im(f"{策略名称} {qmt_code}当前持仓量：{当前持仓量}, 可卖数量为: {卖出数量}, 无法卖出！！！")
            else:
                qu.he_sell_stock(ContextInfo, code, name, 卖出数量, 策略名称)  # 放到前面去设置'是否卖出'=1


def deal_callback(ContextInfo, dealInfo):
    """ 当账号成交状态有变化时，会执行这个函数 """
    qu.deal_callback_func(dealInfo, 策略名称)


def pass_qmt_funcs():
    qu.passorder = passorder
    qu.get_trade_detail_data = get_trade_detail_data
    qu.get_new_purchase_limit = get_new_purchase_limit
    qu.get_ipo_data = get_ipo_data


def stop(ContextInfo):
    log_and_send_im(f"------$$$$$$ {get_curr_date()} {get_curr_time()} {策略名称} 策略已停止！")
