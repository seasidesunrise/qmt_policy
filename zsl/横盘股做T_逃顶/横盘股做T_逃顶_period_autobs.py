# encoding:gbk

"""
横盘股做T_逃顶 策略

"""

import bsea_utils.bsea_xyy_qmt_util as qu
from bsea_utils.bsea_xyy_util import *

策略名称 = '横盘股做T_逃顶'
table_t = 'bsea_横盘股做t_逃顶'

############  人工指定部分开始 ###############

# 数据库字段介绍：见飞书链接 https://z7jmmgj5px.feishu.cn/docx/doxcnDKF1HszxL75RxQ7IpGF1nf

############  人工指定部分结束 ###############

g_countdown_latch = 8
g_data = {}


def timerHandler(ContextInfo):
    curr_time = get_curr_time()

    global g_data

    if not check_is_盘中_or_临近(curr_time):
        print(f"{策略名称} 当前时间不在交易中： {curr_time}")
        return

    sql_all_标的 = "SELECT * FROM " + table_t + " WHERE status='1' AND account_nick='" + str(cst.account_nick) + "'"
    all_df = get_df_from_table(sql_all_标的)
    if len(all_df) == 0:
        print(f"{策略名称} {table_t} 有效标的为空，跳过")

    for index, row in all_df.iterrows():
        qmt_code = row['qmt_code']
        name = qu.get_name_by_qmtcode(ContextInfo, qmt_code)
        观察起始日 = get_dtime_by_datefield(row, '观察起始日dtime')
        print(f"{策略名称} 观察起始日: {观察起始日}")
        是否做t = (get_num_by_numfield(row, '是否做t') == 1)
        成交量放量股数阈值 = get_num_by_numfield(row, '成交量放量股数阈值')
        rt_成交量放量dtime = get_dtime_by_datefield(row, 'rt_成交量放量dtime')
        period = get_str_by_strfield(row, 'period')  # 周期
        if period is None or period not in qu.period_list:
            log_and_send_im(f"{策略名称} {qmt_code}[{name}] period 设置错误，必须为：{qu.period_list} 其中之一，请检查，此条做T策略忽略！！")
            continue

        df = ContextInfo.get_market_data(fields=['volume', 'amount', 'open', 'high', 'low', 'close'], stock_code=[qmt_code], period=period, dividend_type='front', start_time=观察起始日.replace('-', ''))
        df['pre_close'] = df['close'].shift(1)
        df['high涨幅'] = 100 * (df['high'] - df['pre_close']) / df['pre_close']
        print(df)
        where_clause = " WHERE qmt_code='" + qmt_code + "' AND account_nick='" + cst.account_nick + "'"

        ii = 0
        for index2, row2 in df.iterrows():
            ii += 1
            volume = row2['volume'] / 10000  # 转为：万股
            dt = str(index2)
            dt2 = dt[:4] + "-" + dt[4:6] + "-" + dt[6:8] + dt[8:]

            if volume > 成交量放量股数阈值:
                rt_成交量放量dt = dt2

                key = qmt_code + "_成交量放量"
                if g_data.get(key) is None:
                    g_data.update({key: '1'})
                    rt_成交量放量dtime = rt_成交量放量dt
                    log_and_send_im(f"{策略名称} {qmt_code}[{name}] 成交量放量dtime dt:{dt}, rt_成交量放量dtime: {rt_成交量放量dtime}")
                    save_or_update_by_sql("UPDATE " + table_t + " SET rt_成交量放量dtime='" + rt_成交量放量dtime + "', 是否做t='0' " + where_clause)

            if ii < len(df):  # 最后一条数据为盘中，盘中k线未最终确认，不参与计算上影线
                上影线长度与实体倍数 = get_num_by_numfield(row, '上影线长度与实体倍数')
                上影线最高价涨幅 = get_num_by_numfield(row, '上影线最高价涨幅')

                上影线 = (row2['high'] - max(row2['open'], row2['close']))
                实体 = abs(row2['open'] - row2['close'])
                实体 = 实体 if 实体 > 0 else 0.0001
                if 上影线 / 实体 > 上影线长度与实体倍数 > 0 and row2['high涨幅'] > 上影线最高价涨幅 > 0 and dt2 >= rt_成交量放量dtime:  # 上影线比例
                    rt_上影线dtime = dt[:4] + "-" + dt[4:6] + "-" + dt[6:8] + dt[8:]

                    key = qmt_code + "_" + rt_上影线dtime + "_上影线"
                    if g_data.get(key) is None:
                        g_data.update({key: '1'})
                        上影线出现后卖出价与上影线最高价百分比 = get_num_by_numfield(row, '上影线出现后卖出价与上影线最高价百分比')
                        rt_上影线后卖出价格 = row2['high'] * 上影线出现后卖出价与上影线最高价百分比 / 100
                        log_and_send_im(f"{策略名称} {qmt_code}[{name}] 上影线dtime dt:{dt}, rt_上影线dtime: {rt_上影线dtime}")
                        save_or_update_by_sql("UPDATE " + table_t + " SET rt_上影线dtime='" + rt_上影线dtime + "', rt_上影线后卖出价格='" + str(rt_上影线后卖出价格) + "' " + where_clause)


def init(ContextInfo):
    log_and_send_im(f"------$$$$$$ {get_curr_date()}  {get_curr_time()}  {策略名称} 策略已启动init")

    pass_qmt_funcs()
    ContextInfo.set_account(cst.account)

    timer_startTime = get_curr_date() + "09:25:10"
    ContextInfo.run_time("timerHandler", "30nSecond", timer_startTime)


def handlebar(ContextInfo):
    print(f'{策略名称} 这是 handlebar 中的 3秒一次的tick ~~~')
    curr_time = get_curr_time()

    if not check_is_盘中_or_临近(curr_time):
        return

    global g_data

    sql_all_标的 = "SELECT * FROM " + table_t + " WHERE status='1' AND account_nick='" + str(cst.account_nick) + "'"
    all_df = get_df_from_table(sql_all_标的)
    if len(all_df) == 0:
        print(f"{策略名称} 有效标的为空，跳过")

    for index, row in all_df.iterrows():
        qmt_code = row['qmt_code']
        name = qu.get_name_by_qmtcode(ContextInfo, qmt_code)

        做t均线 = get_num_by_numfield(row, '做t均线')
        做t止损均线 = get_num_by_numfield(row, '做t止损均线')
        是否做t = (get_num_by_numfield(row, '是否做t') == 1)  # 人工开关，控制是否需要做t
        高于均线百分比卖出 = get_num_by_numfield(row, '高于均线百分比卖出')  # 如5，即表示高于均线5%卖出
        低于均线百分比买入 = get_num_by_numfield(row, '低于均线百分比买入')  # 如5，即表示低于均线5%买入
        做t资金 = get_num_by_numfield(row, '做t资金')  # 当前做t支配的资金量
        rt_当前做t状态 = get_str_by_strfield(row, 'rt_当前做t状态')
        period = get_str_by_strfield(row, 'period')  # 周期
        if period is None or period not in qu.period_list:
            log_and_send_im(f"{策略名称} {qmt_code}[{name}] period 设置错误，必须为：{qu.period_list} 其中之一，请检查，此条做T策略忽略！！")
            continue
        if 做t均线 <= 1:
            log_and_send_im(f"{策略名称} {qmt_code}[{name}]  做t均线设置错误， 做t均线：{做t均线}，请检查，此条做T策略忽略！！")
            continue
        if 做t止损均线 <= 1:
            log_and_send_im(f"{策略名称} {qmt_code}[{name}] 止损均线设置错误， 做t止损均线：{做t止损均线}，请检查，此条做T策略忽略！！")
            continue

        观察起始日 = str(row['观察起始日dtime'])[:10]
        print(f"{策略名称} 观察起始日: {观察起始日}")
        if 观察起始日 is None:
            log_and_send_im(f"{策略名称} {qmt_code}[{name}] 观察起始日dtime 设置错误，请检查，此条策略忽略！！")
            continue
        elif 观察起始日 > get_curr_date():
            print(f"{策略名称} 观察起始日: {观察起始日} 未到！跳过。。。")
            continue

        if 是否做t:
            df = qu.get_quatation_by_params(ContextInfo, qmt_code, period, 做t均线, 做t止损均线)
            curr_data = df.iloc[-1]
            当前价格 = curr_data['close']
            where_clause = " WHERE qmt_code='" + qmt_code + "' AND account_nick='" + cst.account_nick + "'"

            if 做t止损均线 < 1000 and curr_data['pre_close'] < curr_data['ma' + str(做t止损均线)]:  # 止损(止损均线设置为1000或以上时，不止损)
                # todo:  如果还有未成交的单子，是否要在57分之前先撤单
                卖出数量 = qu.get_可卖股数_by_qmtcode(qmt_code)
                if 卖出数量 == 0:
                    key = qmt_code + "_" + get_curr_date() + "_zs"
                    if g_data.get(key) is None:
                        g_data.update({key: '1'})
                        log_and_send_im(f"{策略名称} {table_t} {qmt_code}[{name}] 达到止损卖出条件，但卖出股数为 0，请人工检查！！")
                    continue
                卖出数量 = 100  # todo：仓位，测试期间暂定100股

                qu.he_sell_stock(ContextInfo, qmt_code, name, 卖出数量, 策略名称)  # 核按钮卖

                save_or_update_by_sql("UPDATE " + table_t + " SET 是否做t='0', status='0' " + where_clause)
                log_and_send_im(f"{策略名称} {table_t} {qmt_code}[{name}] 达到做t止损卖出条件，已下单清仓！！")
                continue
            else:
                相比均线涨幅 = curr_data['相比均线涨幅']
                print(f"{策略名称} 相比均线涨幅: {相比均线涨幅}, 高于均线百分比卖出： {高于均线百分比卖出}, rt_当前做t状态: {rt_当前做t状态}")
                if 相比均线涨幅 >= 高于均线百分比卖出 > 0 and (rt_当前做t状态 == '' or rt_当前做t状态 == '已买回'):  # 做T动作：卖出
                    持仓可卖股数 = qu.get_可卖股数_by_qmtcode(qmt_code)
                    做t卖出股数 = int(做t资金 / 当前价格 / 100) * 100
                    卖出股数 = min(做t卖出股数, 持仓可卖股数)  # 取db中的当前持股数与持仓中的可卖股数，取数字小的那个卖出， todo：当前持股数逻辑需要讨论修改，测试期间先忽略
                    if 卖出股数 == 0:
                        ttl = 10 * 60  # 10分钟
                        log_and_send_im_with_ttl(f"{策略名称} {qmt_code}[{name}] 达到卖出条件，但卖出股数为零。做t卖出股数：{做t卖出股数}, 持仓可卖股数: {持仓可卖股数}", ttl)
                        continue

                    卖出股数 = 100  # todo: 仓位，测试期间暂定100股

                    qu.he_sell_stock(ContextInfo, qmt_code, name, 卖出股数, 策略名称)  # 核按钮卖

                    t_status = T_Type.已t出.value
                    update_sql = "UPDATE " + table_t + " SET rt_当前做t状态='" + t_status + "', rt_当前持股数='" + str(0) + "' " + where_clause
                    save_or_update_by_sql(update_sql)
                    continue

                t出全部成交 = qu.check_委托是否已全部成交(qmt_code)
                if t出全部成交 and (相比均线涨幅 <= -低于均线百分比买入 < 0) and (rt_当前做t状态 == '' or rt_当前做t状态 == '已t出'):  # 做T动作：马上下单买回
                    买入股数 = int(做t资金 / 当前价格 / 100) * 100
                    if 买入股数 < 100:
                        print(f"{策略名称} {qmt_code}[{name}] 达到买入条件，但可买入股数不足一手。买入股数：{买入股数}, 做t资金: {做t资金}")
                        continue
                    买入股数 = 100  # todo: 仓位，测试期间暂定100股

                    qu.he_buy_stock(ContextInfo, qmt_code, name, 买入股数, 策略名称)  # 核按钮买

                    t_status = T_Type.已买回.value
                    update_sql = "UPDATE " + table_t + " SET rt_当前做t状态='" + t_status + "', rt_当前持股数='" + str(买入股数) + "' " + where_clause
                    save_or_update_by_sql(update_sql)
                    continue
        else:
            rt_上影线dtime = get_dtime_by_datefield(row, 'rt_上影线dtime')
            rt_上影线后已触发卖出 = get_num_by_numfield(row, 'rt_上影线后已触发卖出')
            顶部止损均线 = get_num_by_numfield(row, '顶部止损均线')

            df = qu.get_quatation_by_params(ContextInfo, qmt_code, period, 顶部止损均线)
            curr_data = df.iloc[-1]
            当前价格 = curr_data['close']
            where_clause = " WHERE qmt_code='" + qmt_code + "' AND account_nick='" + cst.account_nick + "'"

            if curr_data['pre_close'] < curr_data['ma' + str(顶部止损均线)]:  # 止损
                持仓可卖数量 = qu.get_可卖股数_by_qmtcode(qmt_code)
                db可卖数量 = get_num_by_numfield(row, '跌破止损均线需卖出股数')
                卖出数量 = min(db可卖数量, 持仓可卖数量)
                if 卖出股数 == 0:
                    ttl = 10 * 60  # 10分钟
                    log_and_send_im_with_ttl(f"{策略名称} {qmt_code}[{name}] 达到卖出条件，但卖出股数为零。db可卖数量：{db可卖数量}, 持仓可卖股数: {持仓可卖股数}", ttl)
                    continue

                卖出数量 = 100  # todo: 暂设为100股

                qu.he_sell_stock(ContextInfo, qmt_code, name, 卖出数量, 策略名称)  # 核按钮卖

                save_or_update_by_sql("UPDATE " + table_t + " SET status='0' " + where_clause)
                log_and_send_im(f"{策略名称} {table_t} {qmt_code}[{name}] 达到止损卖出条件，已下单清仓！！")
            else:
                if rt_上影线dtime is not None and rt_上影线后已触发卖出 == 0:  # 按上影线那天价格的98%下单
                    rt_上影线后卖出价格 = get_num_by_numfield(row, 'rt_上影线后卖出价格')
                    if 当前价格 >= rt_上影线后卖出价格:
                        上影线后需卖出股数 = get_num_by_numfield(row, '上影线后需卖出股数')
                        持仓可卖数量 = qu.get_可卖股数_by_qmtcode(qmt_code)
                        卖出数量 = min(上影线后需卖出股数, 持仓可卖数量)
                        if 卖出股数 == 0:
                            ttl = 10 * 60  # 10分钟
                            log_and_send_im_with_ttl(f"{策略名称} {qmt_code}[{name}] 达到卖出条件，但卖出股数为零。上影线后需卖出股数：{上影线后需卖出股数}, 持仓可卖数量: {持仓可卖数量}", ttl)
                            continue

                        卖出数量 = 100  # todo: 测试

                        qu.he_sell_stock(ContextInfo, qmt_code, name, 卖出数量, 策略名称)  # 核按钮卖
                        save_or_update_by_sql("UPDATE " + table_t + " SET rt_上影线后已触发卖出='1' WHERE qmt_code='" + qmt_code + "'")


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
