# encoding:gbk

# 想做一个策略交易，条件：跌停封单大于2亿，30秒内跌停成交量到1亿，封单金额小于5000w，买入

"""
翘板买入策略

"""

import bsea_utils.bsea_xyy_qmt_util as qu
from bsea_utils.bsea_xyy_util import *

cst.webhook = 'https://open.feishu.cn/open-apis/bot/v2/hook/6adbb16f-265f-4a92-bac8-b703b36c7448'

策略名称 = '翘板买入'

############  人工指定部分开始 ###############

g_code_set = []  # 股票标的，需要建一个'翘板买入' 的自定义板块，作为股票池

g_策略总金额 = 10 * 10000  # 该策略最大使用的资金量，单位：元
g_单支股票最大使用金额 = 5 * 10000  # 单支股票做大仓位，单位：元

g_监控秒数 = 30  # 30秒
g_初始跌停封单金额 = 3200  # 初始封单1亿，单位：万元
g_触发买入封单金额 = 2000  # 封单减少到多少时，触发买入，单位：万元
g_监控秒数内至少成交金额 = 1  # 单位：万元

############  人工指定部分结束 ###############

g_countdown_latch = 8
g_prepare_df = g_final_df = pd.DataFrame()
g_今天下过的单_set = set()
g_sell_委托单_num = 0
g_code_set = set()


def recheck_prepare_stocks(ContextInfo):
    print(f'------$$$$$$ {策略名称} timerHandler计时器' + get_curr_date() + " " + get_curr_time())
    global g_code_set
    global g_prepare_df
    g_prepare_df = pd.DataFrame()
    g_code_set = set()

    s = ContextInfo.get_stock_list_in_sector('翘板买入')
    print(f"{策略名称} 翘板买入板块成分股：" + str(s))
    g_code_set = set(s)
    print(f"{策略名称} g_code_set: {g_code_set}")
    print(f"{策略名称} g_code_list: {list(g_code_set)}")

    df_all = ContextInfo.get_market_data(fields=['volume', 'amount', 'open', 'high', 'low', 'close'], stock_code=list(g_code_set), period='1d', dividend_type='front', count=2)
    print(df_all)

    for qmt_code in g_code_set:
        name = qu.get_name_by_qmtcode(ContextInfo, qmt_code)
        涨停价, 跌停价 = qu.get_涨停_跌停价_by_qmt(ContextInfo, qmt_code)

        df = df_all[qmt_code].copy()
        pre1k_data = df.iloc[-2]
        pre_close = pre1k_data['close']  # 设置pre1k收盘价

        print(f"{策略名称} {qmt_code}[{name}]， 跌停价: {fmt_float2str(跌停价)}")
        g_prepare_df = g_prepare_df.append({'qmt_code': qmt_code, '跌停价': 跌停价, 'name': name, 'pre_close': pre_close}, ignore_index=True)

    log_and_send_im(f"{策略名称}，预备股池: {g_prepare_df}")


def init(ContextInfo):
    log_and_send_im(f"------$$$$$$ {get_curr_date()} {get_curr_time()} {策略名称} 策略已启动init")
    pass_qmt_funcs()
    ContextInfo.set_account(cst.account)

    timer_startTime = get_curr_date() + "00:05:10"
    ContextInfo.run_time("recheck_prepare_stocks", "15nSecond", timer_startTime)


def handlebar(ContextInfo):
    print('{策略名称} 这是 handlebar 中的 3秒一次的tick ~~~')

    global g_prepare_df
    global g_final_df
    global g_单支股票最大使用金额
    global g_监控秒数, g_初始跌停封单金额, g_触发封单金额, g_监控秒数内至少成交金额
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

    监控秒数 = g_监控秒数  # 单位：秒
    初始跌停封单金额 = g_初始跌停封单金额  # 单位：万元
    触发买入封单金额 = g_触发封单金额  # 单位：万元
    监控秒数内至少成交金额 = g_监控秒数内至少成交金额  # 单位：万元
    for index2, row2 in g_prepare_df.iterrows():
        qmt_code = row2['qmt_code']
        code = qmt_code[:6]
        pre_close = row2['pre_close']
        name = row2['name']
        跌停价 = row2['跌停价']

        curr_data = df3.get(qmt_code)
        if curr_data is None:
            print(f"get data err: {qmt_code}[{name}]")
            continue

        卖价五档 = curr_data['askPrice']
        卖一价格 = 卖价五档[0]
        买价五档 = curr_data['bidPrice']
        卖量五档 = curr_data['askVol']
        卖一数量 = 卖量五档[0]
        买量五档 = curr_data['bidVol']
        卖一金额 = 卖一价格 * 卖一数量 * 100 / 10000  # 单位：万元
        成交额 = curr_data['amount'] / 10000  # 单位：万元
        成交量 = curr_data['volume']  # 单位：手

        small_flt = 1 / 10000 / 10000
        close = curr_data['lastPrice']
        close = round(close + small_flt, 2)
        跌停价 = round(跌停价 + small_flt, 2)

        print(f"{qmt_code}[{name}] 当前价: {close}, 跌停价: {跌停价},  卖一价格: {卖一价格}, 卖一数量: {卖一数量}, 卖一金额: {卖一金额}, 成交额: {成交额}万, 成交量:{成交量}手, 初始跌停封单金额：{初始跌停封单金额}万,  close <= 跌停价: {close <= 跌停价}")

        if close <= 跌停价 and 卖一金额 >= 初始跌停封单金额:  # 跌停、且封单大于2亿
            has_code = False
            if len(g_final_df) > 0:
                if len(g_final_df[g_final_df['qmt_code'] == qmt_code]) > 0:  # 先删除
                    has_code = True
            if not has_code:
                log_and_send_im(f"{qmt_code}[{name}] 跌停，封单金额大于 {初始跌停封单金额}万，进入监控队列...")
            g_final_df.loc[qmt_code] = {'qmt_code': qmt_code, 'name': name, '初始监控卖一封单': 卖一金额, '初始监控成交额': 成交额, '初始监控成交量': 成交量, '初始监控时间': time.time(), '初始监控时间_dt': get_curr_time()}
            print("############")
            log_and_send_im(f"{qmt_code}[{name}] 跌停，封单金额大于 {g_初始跌停封单金额 / 10000 / 10000}亿，进入监控队列...")
            print(g_final_df)
            return

        if len(g_final_df) > 0:
            curr_final_df = g_final_df[g_final_df['qmt_code'] == qmt_code].copy()
            if len(curr_final_df) > 0:
                curr_final_data = curr_final_df.iloc[0]
                初始监控时间 = curr_final_data['初始监控时间']
                初始监控成交额 = curr_final_data['初始监控成交额']
                当前时间 = time.time()
                if (当前时间 - 初始监控时间 <= 监控秒数) and ((成交额 - 初始监控成交额) > 监控秒数内至少成交金额) and (卖一金额 <= 触发买入封单金额):
                    # 触发买入
                    if qmt_code not in g_今天下过的单_set:
                        g_今天下过的单_set.add(qmt_code)
                        买入价格 = pre_close * (100 - 9) / 100
                        买入股数 = int(g_单支股票最大使用金额 / 买入价格 / 100) * 100
                        买入股数 = 100  # todo：测试期间，统一用100股
                        qu.buy_stock(ContextInfo, qmt_code, name, 买入价格, 买入股数, 策略名称)
                        log_and_send_im(f"{qmt_code}[{name}] 跌停封单金额急剧减少，触发买入，已下单")
                    else:
                        print(f"{qmt_code}[{name}] 今天已下过单，忽略")


def pass_qmt_funcs():
    qu.passorder = passorder
    qu.get_trade_detail_data = get_trade_detail_data
    qu.get_new_purchase_limit = get_new_purchase_limit
    qu.get_ipo_data = get_ipo_data


def stop(ContextInfo):
    log_and_send_im(f"------$$$$$$ {get_curr_date()} {get_curr_time()} {策略名称} 策略已停止！")
