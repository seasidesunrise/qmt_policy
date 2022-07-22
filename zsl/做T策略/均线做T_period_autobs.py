# encoding:gbk

"""
均线做T策略: 高于某根均线5%卖出，低于均线5%买入

主逻辑：
1、高于30线5个点卖出;
2、低于30线5个点买入;
3、跌破60均线止损全部卖出;
4、兼容各种period：'5m', '15m', '30m', '1h', '1d', '1w', '1mon'
5、买入、卖出，均采用2%滑点下单，没成交就没成交了

"""

import bsea_utils.bsea_xyy_qmt_util as qu
from bsea_utils.bsea_xyy_util import *

策略名称 = '均线做T'
table_t = "bsea_做t_均线_period"

############  人工指定部分开始 ###############

g_固定交易100股 = 1  # 值为0时，以数据库配置为准; 为1时，用来测试，即固定100股交易（用来测试，科创板会识别买200股，特例）；


############  人工指定部分结束 ###############


def handlebar(ContextInfo):
    print(f'{策略名称} 这是 handlebar 中的 3秒一次的tick ~~~')
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
        买入最小股数 = get_买入最小股数_by_qmt_code(qmt_code)

        做t均线 = get_num_by_numfield(row, '做t均线')
        做t止损均线 = get_num_by_numfield(row, '做t止损均线')
        高于均线百分比卖出 = get_num_by_numfield(row, '高于均线百分比卖出')  # 如5，即表示高于均线5%卖出
        低于均线百分比买入 = get_num_by_numfield(row, '低于均线百分比买入')  # 如5，即表示低于均线5%买入
        做t资金 = get_num_by_numfield(row, '做t资金')  # 当前做t支配的资金量
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

        df = qu.get_quatation_by_params(ContextInfo, qmt_code, period, 做t均线, 做t止损均线)
        curr_data = df.iloc[-1]
        当前价格 = curr_data['close']
        where_clause = " WHERE qmt_code='" + qmt_code + "' AND account_nick='" + cst.account_nick + "'"

        if 做t止损均线 < 1000 and curr_data['pre_close'] < curr_data['ma' + str(做t止损均线)]:  # 止损
            持仓可卖股数 = qu.get_可卖股数_by_qmtcode(qmt_code)
            做t资金可卖股数 = int(做t资金 / 当前价格 / 100) * 100
            卖出股数 = min(持仓可卖股数, 做t资金可卖股数)
            if 卖出股数 == 0:
                log_and_send_im_with_ttl(f"{策略名称} {qmt_code}[{name}] 达到止损卖出条件，但卖出股数为 0")
                continue
            if g_固定交易100股:
                卖出股数 = 100

            卖出理由 = f"pre1k收盘价跌破{period} {做t止损均线}均线，触发做t止损卖出{卖出股数}股"
            qu.sell_stock_he_2p(ContextInfo, qmt_code, name, 当前价格, 卖出股数, 策略名称, 卖出理由)  # 低于当前价2个点卖出，不成交就不成交

            save_or_update_by_sql("UPDATE " + table_t + " SET status='0' " + where_clause)
            log_and_send_im(f"{策略名称} {qmt_code}[{name}] 达到 做t止损卖出 条件，持仓可卖股数：{持仓可卖股数}, 做t资金可卖股数:{做t资金可卖股数}, 实际下单卖出股数：{卖出股数} ！！")

        else:
            相比均线涨幅 = curr_data['相比均线涨幅']
            # 检查偏离均线幅度
            if (相比均线涨幅 >= 高于均线百分比卖出 > 0) and (rt_当前做t状态 == '' or rt_当前做t状态 == '已买回'):  # 做T动作：卖出
                持仓可卖股数 = qu.get_可卖股数_by_qmtcode(qmt_code)
                做t资金可卖股数 = int(做t资金 / 当前价格 / 100) * 100
                卖出股数 = min(做t资金可卖股数, 持仓可卖股数)  # 取db中的当前持股数与持仓中的可卖股数，取数字小的那个卖出， todo：当前持股数逻辑需要讨论修改，测试期间先忽略
                if 卖出股数 == 0:
                    log_and_send_im_with_ttl(f"{策略名称} {qmt_code}[{name}] 达到卖出条件，但卖出股数为零。做t资金可卖股数：{做t资金可卖股数}, 持仓可卖股数: {持仓可卖股数}")
                    continue
                if g_固定交易100股:
                    卖出股数 = 100

                卖出理由 = f"相比{period} {int(做t均线)}均线涨幅高于{高于均线百分比卖出}%，触发卖出{卖出股数}股"
                qu.sell_stock_he_2p(ContextInfo, qmt_code, name, 当前价格, 卖出股数, 策略名称, 卖出理由)  # 低于当前价2个点卖出，不成交就不成交

                t_status = T_Type.已t出.value
                save_or_update_by_sql("UPDATE " + table_t + " SET rt_当前做t状态='" + t_status + "', rt_当前持股数='" + str(0) + "' " + where_clause)
                log_and_send_im(f"{策略名称} {qmt_code}[{name}] 达到 相比均线涨幅高于均线百分比 卖出条件，持仓可卖股数：{持仓可卖股数}, 做t资金可卖股数:{做t资金可卖股数}, 实际下单卖出股数: {卖出股数}")
                continue

            if (相比均线涨幅 <= -低于均线百分比买入 < 0) and (rt_当前做t状态 == '' or rt_当前做t状态 == '已T出'):  # 做T动作：买回
                t出全部成交 = qu.check_委托是否已全部成交(qmt_code)
                if not t出全部成交:
                    log_and_send_im_with_ttl(f"{策略名称} {qmt_code}[{name}] t出全部成交: {t出全部成交}，等待卖出的单子成交", 30)
                    continue

                账户可用资金 = qu.get_可用资金()
                账户资金最多买入股数 = int(账户可用资金 / 当前价格 / 100) * 100
                做t资金买入股数 = int(做t资金 / 当前价格 / 100) * 100
                买入股数 = min(账户资金最多买入股数, 做t资金买入股数)
                if 买入股数 < 买入最小股数:
                    log_and_send_im_with_ttl(f"{策略名称} {qmt_code}[{name}] 达到买入条件，但可买入股数不足一手。账户资金最多买入股数：{账户资金最多买入股数}, 做t资金买入股数: {做t资金买入股数}")
                    continue
                if g_固定交易100股:
                    买入股数 = 买入最小股数

                买入理由 = f"当前价相比{period} {int(做t均线)}均线涨幅低于：-{低于均线百分比买入}%，触发做t买回{买入股数}股"
                qu.buy_stock_he_2p(ContextInfo, qmt_code, name, 当前价格, 买入股数, 策略名称, 买入理由)

                t_status = T_Type.已买回.value
                save_or_update_by_sql("UPDATE " + table_t + " SET rt_当前做t状态='" + t_status + "', rt_当前持股数='" + str(买入股数) + "' " + where_clause)
                log_and_send_im(f"{策略名称} {qmt_code}[{name}] 达到 价格低于买回价格 买入条件，账户资金最多买入股数：{账户资金最多买入股数}, 做t资金买入股数:{做t资金买入股数}, 实际下单买入股数: {买入股数}")
                continue


def init(ContextInfo):
    global g_固定交易100股
    固定交易100股_msg = "" if not g_固定交易100股 else "->100股模式!!"
    log_and_send_im(f"------$$$$$$ {get_curr_date()}  {get_curr_time()}  {策略名称}  {固定交易100股_msg} 策略已启动init")

    pass_qmt_funcs()
    ContextInfo.set_account(cst.account)


def pass_qmt_funcs():
    qu.passorder = passorder
    qu.get_trade_detail_data = get_trade_detail_data
    qu.get_new_purchase_limit = get_new_purchase_limit
    qu.get_ipo_data = get_ipo_data
    qu.cancel = cancel


def deal_callback(ContextInfo, dealInfo):
    """ 当账号成交状态有变化时，会执行这个函数 """
    qu.deal_callback_func(dealInfo, 策略名称)


def stop(ContextInfo):
    qu.stop_policy(策略名称)
