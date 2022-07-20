"""
 
:Author:  逍遥游
:Create:  2022/7/1$ 10:14$
"""

import talib

from bsea_utils.bsea_xyy_util import *

# qmt 函数
get_trade_detail_data = None
passorder = None
get_new_purchase_limit = None
get_ipo_data = None
cancel = None

# 新股申购
g_新股申购_finish_date_dict = g_逆回购_finish_date_dict = {}  # 格式：account-datetime

# 防重复下单
g_今天下过的单_dict = {}  # 格式：策略-set
g_buy_委托单_num_dict = g_sell_委托单_num_dict = {}  # 格式：策略-num

period_list = ['1m', '3m', '5m', '15m', '30m', '1h', '1d', '1w', '1mon']
small_flt = 1 / 10000 / 10000


def is_当天一字板_by_qmt(ContextInfo, qmt_code, pre_close):
    """ check当天是否一字板 """
    一字板 = False
    涨停价 = 0
    code = qmt_code[:6]
    result_series = ContextInfo.get_market_data(['open', 'high', 'low', 'close', 'volume'], stock_code=[qmt_code], start_time='', end_time='', skip_paused=True, period='1d', dividend_type='none')
    print(result_series)
    if len(result_series) > 0:
        result_series['pre_close'] = pre_close
        当日涨停价, 当日跌停价 = get_涨停_跌停价_by_qmt(ContextInfo, qmt_code)

        if result_series['open'] == result_series['low'] >= 当日涨停价:
            print(code + " 一字涨停板")
            一字板 = True
            涨停价 = result_series['high']

    return 一字板, 涨停价


def get_stock_委托列表(account=cst.account):
    """ 获取股票 stock帐户的现有委托。注意提前登录该帐户，否则无结果返回 """
    obj_list = get_trade_detail_data(account, 'stock', 'ORDER')  # 委托

    print("帐户委托股票为：")
    委托df = pd.DataFrame()
    for obj in obj_list:
        委托日期 = obj.m_strInsertDate
        委托时间 = obj.m_strInsertTime
        委托dtime = f"{委托日期[:4]}-{委托日期[4:6]}-{委托日期[6:8]} {委托时间[0:2]}:{委托时间[2:4]}:{委托时间[4:6]}"

        print(
            "\t证券代码：", obj.m_strInstrumentID, "\t证券名称：", obj.m_strInstrumentName,
            "\t最初委托量：", obj.m_nVolumeTotalOriginal, "\t剩余委托量：", obj.m_nVolumeTotal,
            "\t委托时间：", 委托dtime, "\t委托价格：", obj.m_dLimitPrice,
            "\t买卖标记：", obj.m_strOptName, "\t已撤数量：", obj.m_dCancelAmount,
            "\t成交均价：", obj.m_dTradedPrice, "\t成交金额：", obj.m_dTradeAmountRMB,
            "\t内部委托号：", obj.m_strOrderRef, "\t价格类型：", obj.m_nOrderPriceType,
            "\t委托号：", obj.m_strOrderSysID, "\t废单原因：", obj.m_strCancelInfo,
            "\tuser_order_mark：", obj.m_strRemark,
        )
        委托df = 委托df.append({'qmt_code': obj.m_strInstrumentID, 'code': obj.m_strInstrumentID[:6], 'name': obj.m_strInstrumentName,
                            '最初委托量': obj.m_nVolumeTotalOriginal, '剩余委托量': obj.m_nVolumeTotal, '委托时间': 委托dtime,
                            '委托价格': round(obj.m_dLimitPrice, 2), '买卖标记': obj.m_strOptName, '已撤数量': obj.m_dCancelAmount,
                            '成交均价': round(obj.m_dTradedPrice, 2), '成交金额': round(obj.m_dTradeAmountRMB, 2), '内部委托号': obj.m_strOrderRef,
                            '价格类型': obj.m_nOrderPriceType, '委托号': obj.m_strOrderSysID, '废单原因': obj.m_strCancelInfo,
                            'user_order_mark': obj.m_strRemark
                            }, ignore_index=True)

    return 委托df


def check_委托是否已全部成交(qmt_code):
    df_委托list = get_stock_委托列表()
    if len(df_委托list) == 0:
        return True
    dftmp = df_委托list[df_委托list['qmt_code'] == qmt_code].copy()
    if dftmp is None or len(dftmp) == 0:
        return True
    else:
        return False


def get_可用资金(account=cst.account):
    acct_info = get_trade_detail_data(account, 'stock', 'account')  # 可用资金
    可用资金 = acct_info[0].m_dAvailable
    return 可用资金


def get_stock_持仓列表(account=cst.account):
    """ 获取股票 stock帐户的现有持仓。注意提前登录该帐户，否则无结果返回 """
    acct_info = get_trade_detail_data(account, 'stock', 'account')  # 可用资金
    obj_list = get_trade_detail_data(account, 'stock', 'position')  # 持仓
    可用资金 = 0
    for i in acct_info:
        可用资金 = i.m_dAvailable
        print("帐户可用资金为：", i.m_dAvailable)

    持仓df = pd.DataFrame(columns=['qmt_code', 'code', 'name', '当前持仓量', '可卖数量', '冻结数量', '持仓成本', '当前价', '浮动盈亏', '盈亏比例'])
    print("帐户持有股票为：")
    for obj in obj_list:
        print(
            obj.m_strInstrumentID, obj.m_strInstrumentName,  # 股票代码 和 中文名称
            "\t当前持仓量：", obj.m_nVolume, "\t可卖数量：", obj.m_nCanUseVolume, "\t冻结数量：", obj.m_nFrozenVolume,
            "\t持仓成本：", round(obj.m_dOpenPrice, 2), "\t当前价", obj.m_dSettlementPrice,
            "\t浮动盈亏：", round(obj.m_dFloatProfit, 2), "\t盈亏比例：", round(obj.m_dProfitRate, 3) * 100, "%"
        )
        code = obj.m_strInstrumentID
        qmt_code = get_qmtcode_by_code(code)
        持仓df = 持仓df.append({'qmt_code': qmt_code, 'code': code, 'name': obj.m_strInstrumentName,
                            '当前持仓量': obj.m_nVolume, '可卖数量': obj.m_nCanUseVolume, '冻结数量': obj.m_nFrozenVolume,
                            '持仓成本': round(obj.m_dOpenPrice, 2), '当前价': obj.m_dSettlementPrice, '浮动盈亏': round(obj.m_dFloatProfit, 2),
                            '盈亏比例': round(obj.m_dProfitRate, 3) * 100}, ignore_index=True)
    print(持仓df)
    return 可用资金, 持仓df, obj_list


def get_可卖股数_by_qmtcode(qmt_code, account=cst.account):
    可用资金, 持仓df, obj_list = get_stock_持仓列表(account)
    if len(持仓df) == 0:
        return 0
    持仓df2 = 持仓df[持仓df['qmt_code'] == qmt_code].copy()
    if len(持仓df2) == 0:
        return 0
    else:
        return 持仓df2.iloc[0]['可卖数量']


def get_name_by_qmtcode(ContextInfo, qmt_code):
    return ContextInfo.get_stock_name(qmt_code)


def deal_callback_func(dealInfo, 策略名称):
    """ 成交回调函数 """
    strRemark = dealInfo.m_strRemark
    if 策略名称 in strRemark:
        证券代码 = dealInfo.m_strInstrumentID
        证券名称 = dealInfo.m_strInstrumentName
        成交编号 = dealInfo.m_strTradeID
        买卖标记 = dealInfo.m_strOptName

        成交日期tmp = dealInfo.m_strTradeDate
        成交时间tmp = dealInfo.m_strTradeTime
        if len(成交时间tmp) == 5:
            成交时间tmp = '0' + 成交时间tmp
        成交时间 = 成交时间tmp[:2] + ":" + 成交时间tmp[2:4] + ":" + 成交时间tmp[4:]
        成交日期 = 成交日期tmp[:4] + "-" + 成交日期tmp[4:6] + "-" + 成交日期tmp[6:]

        成交量 = dealInfo.m_nVolume
        成交均价 = fmt_float2str(dealInfo.m_dPrice)
        成交额 = fmt_float2str(dealInfo.m_dTradeAmount)
        手续费 = fmt_float2str(dealInfo.m_dComssion)
        txt1 = f"[{策略名称}] 成交：{成交日期} {成交时间}  {买卖标记}  {证券代码}[{证券名称}]  {成交量}股,  价格: {成交均价},  金额: {成交额},  手续费:{手续费}, strRemark: {strRemark}"
        log_and_send_im(txt1)


def get_qmtcode_by_code(code):
    if code is not None and len(code) == 6:
        qmt_code = code
        if code[:1] == '6':
            qmt_code = code + ".SH"
        if code[:1] == '3' or code[:1] == '0':
            qmt_code = code + ".SZ"
        return qmt_code
    return code


def 国债逆回购(ContextInfo, account=cst.account):
    global g_逆回购_finish_date_dict

    curr_date = get_curr_date()
    curr_time = get_curr_time()
    curr_dtime = curr_date + " " + curr_time

    逆回购_finish_date = g_逆回购_finish_date_dict.get(account)
    if 逆回购_finish_date is not None and 逆回购_finish_date == curr_date:
        log_and_send_im_with_ttl(f"检测到重复逆回购，跳过: {g_逆回购_finish_date_dict}", 600)
        return
    print(f"{curr_dtime} 国债逆回购: {g_逆回购_finish_date_dict}")
    acct_info = get_trade_detail_data(account, 'stock', 'account')
    可用资金 = acct_info[0].m_dAvailable
    log_and_send_im(f"帐户可用资金为：{可用资金}")

    shiwan_cnt = int(可用资金 / 100000)
    yiqian_cnt = int((可用资金 - 100000 * shiwan_cnt) / 1000)
    shiwan_手数 = shiwan_cnt * 1000
    yiqian_手数 = yiqian_cnt * 10

    R_001_code = '131810.SZ'  # 深圳市场：1000元10手
    GC_001_code = '204001.SH'  # 上海市场：10w元1000手

    if shiwan_手数 > 0:
        code = GC_001_code
        log_and_send_im(f'卖出国债逆回购 {code} 金额：{shiwan_cnt * 10}万，手数: {shiwan_手数}')
        passorder(24, 1101, account, code, 5, -1, shiwan_手数, '国债逆回购', 1, '国债逆回购_gc001_oid', ContextInfo)
    if yiqian_手数 > 0:
        code = R_001_code
        log_and_send_im(f'卖出国债逆回购 {code} 金额：{yiqian_cnt / 10}万，手数: {yiqian_手数}')
        passorder(24, 1101, account, R_001_code, 5, -1, yiqian_手数, '国债逆回购', 1, '国债逆回购_r001_oid', ContextInfo)
    逆回购_finish_date = get_curr_date()
    g_逆回购_finish_date_dict.update({account: 逆回购_finish_date})
    log_and_send_im(f'今日国债逆回购完成，g_finish_date: {g_逆回购_finish_date_dict}')


def 新股_新债_申购(ContextInfo, account=cst.account):
    global g_新股申购_finish_date_dict
    新股申购_finish_date = g_新股申购_finish_date_dict.get(account)
    if 新股申购_finish_date is not None and 新股申购_finish_date == get_curr_date():
        print("已经完成新股申购")
        return

    clm = 'bsea_qmt_新股申购'
    rgipoed = get_new_purchase_limit(account)
    print(f"今日可申购新股的额度：{rgipoed}")  # 今日可申购新股的额度：{'KCB': 3500, 'SH': 3000, 'SZ': 20000}, 单位股

    ipoCB = get_ipo_data("BOND")
    print(f"今日可申购可转债：{ipoCB}")
    if ipoCB:
        for sec_code in ipoCB.keys():
            log_and_send_im('认购新可转债 {} {} 数量：{}'.format(sec_code, ipoCB[sec_code]['name'], ipoCB[sec_code]['maxPurchaseNum']))
            passorder(23, 1101, account, sec_code, 11, 100, 10000, clm, 2, 'IPOBOND', ContextInfo)

    ipoStock = get_ipo_data("STOCK")  # 返回新股信息
    print(f"今日可申购新股：{ipoStock}")
    if ipoStock:
        for sec_code in ipoStock.keys():  # sec_code取值形如：'001226.SZ'
            if sec_code[:2] != '88' and sec_code[:2] != '78' and sec_code[:2] != '30':  # 不申购北交所、科创板和创业板的品种： 78是科创板申购代码前缀，30是创业板申购代码前缀
                bk = sec_code[-2:]  # 获得申购代码后缀，比如SH，或SZ
                if bk == 'SH' and sec_code[:3] == '787':
                    bk = 'KCB'
                print('申购新股代码：{} 板块：{}'.format(sec_code, bk))
                print('申购额度：{} 最大申购数量：{} 申购数量：{}'.format(rgipoed[bk], ipoStock[sec_code]['maxPurchaseNum'], min(rgipoed[bk], ipoStock[sec_code]['maxPurchaseNum'])))
                申购数量 = min(rgipoed[bk], ipoStock[sec_code]['maxPurchaseNum'])  # 申购数量
                if 申购数量 != 0:
                    log_and_send_im('认购新股 {} {} 数量：{} 价格：{}'.format(sec_code, ipoStock[sec_code]['name'], 申购数量, ipoStock[sec_code]['issuePrice']))
                    passorder(23, 1101, account, sec_code, 11, ipoStock[sec_code]['issuePrice'], 申购数量, clm, 2, 'IPOSTOCK', ContextInfo)

    新股申购_finish_date = get_curr_date()
    g_新股申购_finish_date_dict.update({account: 新股申购_finish_date})
    log_and_send_im(f'今日自动申购新股、新可转债完成，g_新股申购_finish_date: {g_新股申购_finish_date_dict}')


def buy_stock(ContextInfo, qmt_code, name, 买入价格, 买入股数, 策略):
    """ 买入下单 """
    global g_今天下过的单_dict
    今天下过的单_set = g_今天下过的单_dict.get(策略)
    今天下过的单_set = 今天下过的单_set if 今天下过的单_set is not None else set()
    买单_unique_str = f"{qmt_code}_{买入股数}_{get_curr_date()}_{策略}"
    if 买单_unique_str in 今天下过的单_set:
        print("重复下委托单，买单_unique_str：" + 买单_unique_str + ", 忽略此笔委托")
    else:
        今天下过的单_set.add(买单_unique_str)  # todo：是否会有线程安全问题，先忽略
        g_今天下过的单_dict.update({策略: 今天下过的单_set})
        log_and_send_im(f"{策略} 委托买入 {qmt_code}  {name}  {买入股数}  股, 委托价格：{买入价格}, 委托Id: {买单_unique_str}")
        passorder(23, 1101, cst.account, qmt_code, 11, 买入价格, 买入股数, 策略, 1, 买单_unique_str, ContextInfo)  # 买入


def buy_stock_he(ContextInfo, qmt_code, name, 买入股数, 策略):
    """ 核按钮买入下单 """
    if qmt_code.startswith('688') or qmt_code.startswith('3'):
        # 取当前价
        当前价格 = get_curr_price(ContextInfo, qmt_code)
        buy_stock_he_2p(ContextInfo, qmt_code, name, 当前价格, 买入股数, 策略)
    else:
        当日涨停价, 当日跌停价 = get_涨停_跌停价_by_qmt(ContextInfo, qmt_code)
        买入价格 = 当日涨停价
        buy_stock(ContextInfo, qmt_code, name, 买入价格, 买入股数, 策略)


def buy_stock_he_2p(ContextInfo, qmt_code, name, 当前价格, 买入股数, 策略):
    买入价格 = (1 + 0.019) * 当前价格
    买入价格 = round(买入价格 + small_flt, 2)
    buy_stock(ContextInfo, qmt_code, name, 买入价格, 买入股数, 策略)


def get_涨停_跌停价_by_qmt(ContextInfo, qmt_code):
    obj = ContextInfo.get_instrumentdetail(qmt_code)
    当日涨停价 = obj['UpStopPrice']
    当日跌停价 = obj['DownStopPrice']
    return 当日涨停价, 当日跌停价


def sell_stock(ContextInfo, qmt_code, name, 卖出价格, 卖出数量, 策略):
    """ 卖出下单 """
    global g_sell_委托单_num_dict
    sell_委托单_num = get_num_by_numfield(g_sell_委托单_num_dict, 策略)
    sell_委托单_num += 1
    sell_order_id = 策略 + '_SELL_ORDER_' + str(sell_委托单_num)
    g_sell_委托单_num_dict.update({策略: sell_委托单_num})

    log_and_send_im(f"{策略} 委托卖出 {qmt_code}  {name} {卖出数量} 股, 委托价格：{卖出价格}, sell_order_id: {sell_order_id}")
    passorder(24, 1101, cst.account, qmt_code, 11, 卖出价格, 卖出数量, 策略, 1, sell_order_id, ContextInfo)  # 卖出


def sell_stock_he(ContextInfo, qmt_code, name, 卖出数量, 策略):
    if qmt_code.startswith('688') or qmt_code.startswith('3'):
        # 取当前价
        当前价格 = get_curr_price(ContextInfo, qmt_code)
        sell_stock_he_2p(ContextInfo, qmt_code, name, 当前价格, 卖出数量, 策略)
    else:
        当日涨停价, 当日跌停价 = get_涨停_跌停价_by_qmt(ContextInfo, qmt_code)
        卖出价格 = 当日跌停价
        sell_stock(ContextInfo, qmt_code, name, 卖出价格, 卖出数量, 策略)


def sell_stock_he_2p(ContextInfo, qmt_code, name, 当前价格, 卖出数量, 策略):
    卖出价格1 = (1 - 0.019) * 当前价格
    卖出价格 = round(卖出价格1 - small_flt, 2)

    sell_stock(ContextInfo, qmt_code, name, 卖出价格, 卖出数量, 策略)


def cancel_all_order(ContextInfo, 策略, account=cst.account):
    global g_sell_委托单_num_dict
    sell_委托单_num = get_num_by_numfield(g_sell_委托单_num_dict, 策略)
    print("撤掉所有的单，准备集合竞价成交, sell_委托单_num: " + str(sell_委托单_num))
    for num in range(sell_委托单_num):
        orderId = 策略 + '_SELL_ORDER_' + str(num + 1)
        log_and_send_im(f"撤销挂单, orderId: {orderId}")
        cancel(orderId, account, 'STOCK')


def get_preclose_day_by_qmtcode(ContextInfo, qmt_code):
    """ 取昨日收盘价，取不到返回-1 """
    zuotian_date = get_zuotian_date()
    df = ContextInfo.get_market_data(fields=['volume', 'close'], stock_code=[qmt_code], period='1d', dividend_type='front', count=1, end_time=zuotian_date.replace('-', ''))

    pre_close = -1
    if len(df) > 0:
        pre_close = df.iloc[0]['close']
    print(f"zuotian_date: {zuotian_date}, pre_close: {pre_close}")
    return pre_close


def stop_policy(策略名称):
    log_and_send_im(f"------$$$$$$ {get_curr_date()}  {get_curr_time()}  {策略名称} 策略已停止！")


def get_quatation_by_params(ContextInfo, qmt_code, period, 做t均线, 止损均线=None):
    止损均线_无效值 = 1000
    cnt = 做t均线 if (止损均线 is None or 止损均线 >= 止损均线_无效值) else max(做t均线, 止损均线)
    endtime = get_curr_date().replace('-', '') + "150000"
    df = ContextInfo.get_market_data(['volume', 'amount', 'open', 'high', 'low', 'close'], stock_code=[qmt_code], period=period, dividend_type='front', count=int(cnt + 10), end_time=endtime)
    ma_colname = 'ma' + str(做t均线)
    df[ma_colname] = talib.MA(df['close'], 做t均线)
    if 止损均线 is not None and 止损均线 < 止损均线_无效值:
        df['ma' + str(止损均线)] = talib.MA(df['close'], 止损均线)
    df['pre_close'] = df['close'].shift(1)
    df['涨幅'] = 100 * (df['close'] - df['pre_close']) / df['pre_close']
    df['相比均线涨幅'] = 100 * (df['close'] - df[ma_colname]) / df[ma_colname]
    return df


def get_curr_price(ContextInfo, qmt_code):
    endtime = get_curr_date().replace('-', '') + "150000"
    df = ContextInfo.get_market_data(['volume', 'close'], stock_code=[qmt_code], period='1d', dividend_type='front', count=1, end_time=endtime)
    close = None
    if len(df) > 0:
        close = df.iloc[0]['close']
    return close
