# encoding:gbk

"""
定价条件单策略

作用：主要用来建仓，执行一次此条条件单即告失效。

"""

import bsea_utils.bsea_xyy_qmt_util as qu
from bsea_utils.bsea_xyy_util import *

策略名称 = '定价BS条件单'
table_t = 'bsea_定价bs条件单'


def handlebar(ContextInfo):
    print(f'这是 {策略名称} handlebar 中的 3秒一次的tick ~~~')

    curr_date = get_curr_date()
    curr_time = get_curr_time()
    curr_dtime = curr_date + " " + curr_time

    sql_all_标的 = "SELECT * FROM " + table_t + " WHERE status='1' AND account_nick='" + str(cst.account_nick) + "'"
    all_df = get_df_from_table(sql_all_标的)
    if len(all_df) == 0:
        print(f"{curr_dtime} {策略名称} {table_t} 有效标的为空，跳过")
        return

    print(all_df)
    curr_date = get_curr_date()
    for index, row in all_df.iterrows():
        qmt_code = row['qmt_code']
        name = qu.get_name_by_qmtcode(ContextInfo, qmt_code)

        pk_id = get_num_by_numfield(row, 'id')
        条件单类型 = get_str_by_strfield(row, '条件单类型')
        if 条件单类型 != 定价条件单.定价买入.value and 条件单类型 != 定价条件单.定价卖出.value:
            log_and_send_im_with_ttl(f"{curr_dtime} {pk_id} 条件单类型: {条件单类型} 配置错误，请检查！")
            continue

        df = ContextInfo.get_market_data(fields=['volume', 'close'], stock_code=[qmt_code], period='1d', dividend_type='front', count=1)
        if len(df) == 0 or df.iloc[0]['volume'] == 0:  # 判断volume是为了过滤停牌
            log_and_send_im_with_ttl(f"{curr_dtime} {策略名称} {qmt_code}[{name}] 获取行情数据失败，跳过")
            continue
        curr_data = df.iloc[0]
        当前价格 = curr_data['close']
        where_clause = " WHERE id='" + str(pk_id) + "'"

        if 条件单类型 == 定价条件单.定价买入.value:
            买入价格 = get_num_by_numfield(row, '交易价格')
            买入数量 = get_num_by_numfield(row, '交易股数')
            买入截止有效期db = get_dtime_by_datefield(row, '交易截止日期')
            买入截止有效期 = 买入截止有效期db if 买入截止有效期db is not None else curr_date
            is_valid_买入配置 = False
            if 买入价格 > 0 and 买入数量 >= 100 and 买入截止有效期 >= curr_date:
                is_valid_买入配置 = True

            if not is_valid_买入配置:
                log_and_send_im_with_ttl(f"{curr_dtime} {策略名称} {qmt_code}[{name}] {买入价格} {买入数量} {买入截止有效期}  {int(pk_id)}此条配置无效，请检查！当天跳过。")
                continue
            else:
                if 当前价格 <= 买入价格:
                    账户可用资金 = qu.get_可用资金()
                    资金最多买入股数 = int(账户可用资金 / 当前价格 / 100) * 100
                    买入股数 = min(买入数量, 资金最多买入股数)
                    if 买入股数 < 100:
                        log_and_send_im_with_ttl(f"{curr_dtime} {策略名称} {qmt_code}[{name}] 达到买入条件，但买入股数为零。db买入股数：{买入数量}, 资金最多买入股数: {资金最多买入股数}, 账户可用资金: {账户可用资金}")
                        continue
                    买入股数 = 100  # todo: 待删除

                    qu.buy_stock_he(ContextInfo, qmt_code, name, 买入股数, 策略名称)

                    update_sql = "UPDATE " + table_t + " SET status='0', lastmodified='" + get_lastmodified() + "'" + where_clause
                    save_or_update_by_sql(update_sql)


        elif 条件单类型 == 定价条件单.定价卖出.value:
            卖出价格 = get_num_by_numfield(row, '交易价格')
            卖出数量 = get_num_by_numfield(row, '交易股数')
            卖出截止有效期_db = get_dtime_by_datefield(row, '交易截止日期')
            卖出截止有效期 = 卖出截止有效期_db if 卖出截止有效期_db is not None else curr_date
            is_valid_卖出配置 = False
            if 卖出价格 > 0 and 卖出数量 >= 100 and 卖出截止有效期 >= curr_date:
                is_valid_卖出配置 = True

            if not is_valid_卖出配置:
                log_and_send_im_with_ttl(f"{curr_dtime} {策略名称} {qmt_code}[{name}] {买入价格} {买入数量} {买入截止有效期} {int(pk_id)}此条配置无效，请检查！当天跳过。")
                continue
            else:
                if 当前价格 >= 卖出价格:
                    当前持股数 = qu.get_可卖股数_by_qmtcode(qmt_code)
                    卖出股数 = min(卖出数量, 当前持股数)
                    if 卖出股数 < 100:
                        log_and_send_im_with_ttl(f"{curr_dtime} {策略名称} {qmt_code}[{name}] 达到卖出条件，但卖出股数为零。db卖出股数：{卖出数量}, 持仓可卖股数: {当前持股数}")
                        continue
                    卖出股数 = 100  # todo：待删除

                    qu.sell_stock_he(ContextInfo, qmt_code, name, 卖出股数, 策略名称)

                    update_sql = "UPDATE " + table_t + " SET status='0', lastmodified='" + get_lastmodified() + "'" + where_clause
                    save_or_update_by_sql(update_sql)


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
