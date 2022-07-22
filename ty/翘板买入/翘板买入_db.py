# encoding:gbk

# 翘板买入策略
# 想做一个策略交易，条件：跌停封单大于2亿，30秒内跌停成交量到1亿，封单金额小于5000w，买入
# 如果把  ”初始跌停封单金额“ 设置成0， 且监控秒数为0，则此时直接判断卖一封单，小于 ”触发买入封单金额“  就委托买入；如果监控秒数不为0，则监控比如30秒内跌停成交额到1亿，封单金额小于5000w，这样的逻辑；


# 集合竞价期间数据排除 卖一不准
# 触发买入封单金额：为0的情况
#
#

import bsea_utils.bsea_xyy_qmt_util as qu
from bsea_utils.bsea_xyy_util import *

# cst.webhook = 'https://open.feishu.cn/open-apis/bot/v2/hook/6adbb16f-265f-4a92-bac8-b703b36c7448'

策略名称 = '翘板买入'
table_t = 'bsea_翘板买入'
g_data = {}

############  人工指定部分开始 ###############

g_固定交易100股 = 1  # 值为0时，以数据库配置为准; 为1时，用来测试，即固定100股交易（用来测试，科创板会识别买200股，特例）；

############  人工指定部分结束 ###############

g_countdown_latch = 8
g_prepare_df = pd.DataFrame(columns=['qmt_code', 'name', '买入资金', '跌停价', 'pre_close', '监控秒数', '初始跌停封单金额', '触发买入封单金额', '监控秒数内至少成交金额'])
g_今天下过的单_set = set()


def recheck_prepare_stocks(ContextInfo):
    curr_time = get_curr_time()
    print(f'------$$$$$$ {策略名称} timerHandler计时器' + get_curr_date() + " " + curr_time)

    if not check_is_盘中_or_临近(curr_time):
        print(f"{策略名称} 当前时间不在交易中： {curr_time}")
        return

    sql_all_标的 = "SELECT * FROM " + table_t + " WHERE status='1' AND account_nick='" + str(cst.account_nick) + "'"
    all_df = get_df_from_table(sql_all_标的)
    if len(all_df) == 0:
        print(f"{策略名称} {table_t} 有效标的为空，跳过")
    for index11, row11 in all_df.iterrows():
        code = row11['code']
        qmt_code = qu.get_qmtcode_by_code(code)
        all_df.loc[index11, 'qmt_code'] = qmt_code

    global g_prepare_df
    df_all = ContextInfo.get_market_data(fields=['volume', 'amount', 'open', 'high', 'low', 'close'], stock_code=all_df['qmt_code'].tolist(), period='1d', dividend_type='front', count=2)
    print(df_all)
    for index, row in all_df.iterrows():
        code = row['code']
        qmt_code = qu.get_qmtcode_by_code(code)
        买入资金 = row['买入资金']
        监控秒数 = row['监控秒数']
        初始跌停封单金额 = row['初始跌停封单金额']
        触发买入封单金额 = row['触发买入封单金额']
        监控秒数内至少成交金额 = row['监控秒数内至少成交金额']
        name = qu.get_name_by_qmtcode(ContextInfo, qmt_code)
        涨停价, 跌停价 = qu.get_涨停_跌停价_by_qmt(ContextInfo, qmt_code)

        df = df_all[qmt_code].copy()
        pre1k_data = df.iloc[-2]
        pre_close = pre1k_data['close']  # 设置pre1k收盘价

        print(f"{策略名称} {qmt_code}[{name}]， 跌停价: {fmt_float2str(跌停价)}")
        g_prepare_df.loc[qmt_code] = {'qmt_code': qmt_code, 'name': name, '买入资金': 买入资金, '跌停价': 跌停价, 'pre_close': pre_close, '监控秒数': 监控秒数, '初始跌停封单金额': 初始跌停封单金额, '触发买入封单金额': 触发买入封单金额, '监控秒数内至少成交金额': 监控秒数内至少成交金额}

    print(f"{策略名称}，预备股池: {g_prepare_df}")


def init(ContextInfo):
    global g_固定交易100股
    固定交易100股_msg = "" if not g_固定交易100股 else "->100股模式!!"
    log_and_send_im(f"------$$$$$$ {get_curr_date()}  {get_curr_time()}  {策略名称}  {固定交易100股_msg} 策略已启动init")

    pass_qmt_funcs()
    ContextInfo.set_account(cst.account)

    timer_startTime = get_curr_date() + "00:05:10"
    ContextInfo.run_time("recheck_prepare_stocks", "15nSecond", timer_startTime)


def is_正常翘板时间(curr_time):
    """ 9点33分到尾盘14点54分为翘板工作时间，其它时间忽略 """
    if curr_time >= '09:33:00' and curr_time < '14:54:00':
        return True
    else:
        print(f"{策略名称} 当前时间 {curr_time} 不在翘板工作时间[09:33 ~ 14:54]")
        return False


def handlebar(ContextInfo):
    print('{策略名称} 这是 handlebar 中的 3秒一次的tick ~~~')

    curr_date = get_curr_date()
    curr_time = get_curr_time()
    curr_dtime = curr_date + " " + curr_time

    if not is_正常翘板时间(curr_time):
        return

    global g_prepare_df
    global g_今天下过的单_set
    global g_countdown_latch
    global g_data

    g_countdown_latch -= 1
    if g_countdown_latch <= 0:
        g_countdown_latch = 8
        可用资金, 持仓df, obj_list = qu.get_stock_持仓列表(cst.account)

    if len(g_prepare_df) == 0:
        return

    df3 = ContextInfo.get_full_tick(stock_code=g_prepare_df['qmt_code'].tolist())

    for index2, row2 in g_prepare_df.iterrows():
        qmt_code = row2['qmt_code']
        code = qmt_code[:6]
        pre_close = row2['pre_close']
        买入资金 = row2['买入资金']
        name = row2['name']
        跌停价 = row2['跌停价']
        监控秒数 = row2['监控秒数']  # 单位：秒
        初始跌停封单金额 = row2['初始跌停封单金额']  # 单位：万元
        触发买入封单金额 = row2['触发买入封单金额']  # 单位：万元
        监控秒数内至少成交金额 = row2['监控秒数内至少成交金额']  # 单位：万元
        买入最小股数 = qu.get_买入最小股数_by_qmt_code(qmt_code)

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
        卖一金额 = 卖一价格 * 卖一数量 * 100 / 10000  # 单位：万
        成交额 = curr_data['amount'] / 10000  # 单位：万
        成交量 = curr_data['volume']  # 单位：手

        small_flt = 1 / 10000 / 10000
        close = curr_data['lastPrice']
        close = round(close + small_flt, 2)
        跌停价 = round(跌停价 + small_flt, 2)

        print(f"{qmt_code}[{name}] 当前价: {close}, 跌停价: {跌停价},  卖一价格: {卖一价格}, 卖一数量: {卖一数量}, 卖一金额: {卖一金额}, 成交额: {成交额}万, 成交量:{成交量}手, 初始跌停封单金额：{初始跌停封单金额}万,  close <= 跌停价: {close <= 跌停价}")

        g_final_df = g_data.get(qmt_code)
        if g_final_df is None:
            g_final_df = pd.DataFrame(columns=['qmt_code', 'name', 'curr_time', '卖一金额', '当前总成交额', '当前总成交量', '初始监控卖一封单', '初始监控成交额', '初始监控成交量', '初始监控时间', '初始监控时间_dt'])
        if 初始跌停封单金额 == 0:
            if 监控秒数 == 0:  # 监控秒数为0， 只看'触发买入封单金额'条件是否满足
                if 卖一金额 <= 触发买入封单金额:
                    # 触发买入
                    if qmt_code not in g_今天下过的单_set:
                        g_今天下过的单_set.add(qmt_code)
                        买入价格 = pre_close * (100 - 9) / 100
                        买入股数 = int(买入资金 / 买入价格 / 100) * 100
                        if g_固定交易100股:
                            买入股数 = 买入最小股数

                        qu.buy_stock(ContextInfo, qmt_code, name, 买入价格, 买入股数, 策略名称)
                        log_and_send_im(f"{qmt_code}[{name}] 初始跌停封单金额=0， 且跌停封单小于触发买入封单，触发买入，已下单, 买入价格：{fmt_float2str(买入价格)}, 买入股数: {买入股数}")
                    else:
                        print(f"{qmt_code}[{name}] 今天已下过单，忽略")
            else:
                # 只有'初始跌停封单金额'字段为0，其它条件正常判断
                if close <= 跌停价:  # 跌停
                    log_and_send_im(f"{qmt_code}[{name}] 跌停，封单金额大于 {初始跌停封单金额}万，进入监控队列...")
                    g_final_df.loc[curr_time] = {'qmt_code': qmt_code, 'name': name, 'curr_time': curr_time, '卖一金额': 卖一金额, '当前总成交额': 成交额, '当前总成交量': 成交量}
                    g_data[qmt_code] = g_final_df

                    cnt = 监控秒数 / 3
                    if len(g_final_df) > cnt:
                        data_start = g_final_df.iloc[cnt - 1]
                        data_curr = g_final_df.iloc[0]
                        if data_curr['当前总成交额'] - data_start['当前总成交额'] >= 监控秒数内至少成交金额:  # 触发买入
                            if qmt_code not in g_今天下过的单_set:
                                g_今天下过的单_set.add(qmt_code)
                                买入价格 = pre_close * (100 - 9) / 100
                                买入股数 = int(买入资金 / 买入价格 / 100) * 100
                                if g_固定交易100股:
                                    买入股数 = 买入最小股数

                                qu.buy_stock(ContextInfo, qmt_code, name, 买入价格, 买入股数, 策略名称)
                                log_and_send_im(f"{qmt_code}[{name}] 跌停封单金额急剧减少，触发买入，已下单, 买入价格：{fmt_float2str(买入价格)}, 买入股数: {买入股数}")
                            else:
                                print(f"{qmt_code}[{name}] 今天已下过单，忽略")
                continue
        else:
            if close <= 跌停价 and 卖一金额 >= 初始跌停封单金额:  # 跌停、且封单大于2000万
                has_code = False
                if len(g_final_df) > 0:
                    if len(g_final_df[g_final_df['qmt_code'] == qmt_code]) > 0:  # 先删除
                        has_code = True
                if not has_code:
                    log_and_send_im(f"{qmt_code}[{name}] 跌停，封单金额大于 {初始跌停封单金额}万，进入监控队列...")
                g_final_df.loc[qmt_code] = {'qmt_code': qmt_code, 'name': name, '初始监控卖一封单': 卖一金额, '初始监控成交额': 成交额, '初始监控成交量': 成交量, '初始监控时间': time.time(), '初始监控时间_dt': get_curr_time()}
                print("############")
                print(g_final_df)
                return

            if len(g_final_df) > 0:
                curr_final_df = g_final_df[g_final_df['qmt_code'] == qmt_code].copy()
                if len(curr_final_df) > 0:
                    curr_final_data = curr_final_df.iloc[0]
                    初始监控时间 = curr_final_data['初始监控时间']
                    初始监控成交额 = curr_final_data['初始监控成交额']
                    当前时间 = time.time()
                    print(f"--->>>>>{qmt_code}[{name}] cond1: {当前时间 - 初始监控时间 <= 监控秒数}, 监控秒数: {监控秒数},  cond2: {((成交额 - 初始监控成交额) > 监控秒数内至少成交金额)}, cond3: {(卖一金额 <= 触发买入封单金额)}")
                    if (当前时间 - 初始监控时间 <= 监控秒数) and ((成交额 - 初始监控成交额) > 监控秒数内至少成交金额) and (卖一金额 <= 触发买入封单金额):
                        # 触发买入
                        if qmt_code not in g_今天下过的单_set:
                            g_今天下过的单_set.add(qmt_code)
                            买入价格 = pre_close * (100 - 9) / 100
                            买入股数 = int(买入资金 / 买入价格 / 100) * 100
                            if g_固定交易100股:
                                买入股数 = 买入最小股数

                            qu.buy_stock(ContextInfo, qmt_code, name, 买入价格, 买入股数, 策略名称)
                            log_and_send_im(f"{qmt_code}[{name}] 跌停封单金额急剧减少，触发买入，已下单, 买入价格：{fmt_float2str(买入价格)}, 买入股数: {买入股数}")
                        else:
                            print(f"{qmt_code}[{name}] 今天已下过单，忽略")


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
