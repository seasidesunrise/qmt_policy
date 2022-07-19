# encoding:gbk

"""
qmt 下单程序

包含集合竞价后读取buy_info数据表买入下单，和开盘后读取sell_info表，择机卖出
包含新股、新债自动申购、国债逆回购（09:59分开始）

所谓择机卖出，包含三个逻辑：
1、开盘需要卖出的（开盘卖出=1），竞价核按钮卖出
2、盘中摸跌停、或涨停，均卖出
3、一字板不破，持股不卖；一字板开板，立即核按钮
4、非以上情况，集合竞价挂跌停卖出

"""

import bsea_utils.bsea_xyy_qmt_util as qu
from bsea_utils.bsea_xyy_util import *

策略名称 = 'BSEA'

bsea_buy_table_t = 'bsea_buy_info'
bsea_sell_table_t = 'bsea_sell_info'

g_countdown_latch = 0
g_一字板_df = pd.DataFrame()


def timerHandler(ContextInfo):
    curr_time = get_curr_time()
    curr_date = get_curr_date()

    global g_一字板_df
    print(f'------$$$$$$ {策略名称} timerHandler计时器 {curr_date} {curr_time}')

    if curr_time > '09:22:20' and curr_time < '09:25:00':  # 将昨日跌停标的卖出
        # 处理需要开盘卖出的股票
        sell_df2 = get_df_from_table("SELECT * FROM " + bsea_sell_table_t + " WHERE sell_dtime='" + get_curr_date() + "' AND 开盘卖出=1 AND status=1 ORDER BY 策略 ASC, 子策略 ASC")
        print(sell_df2)
        if len(sell_df2) == 0:
            print(f"{策略名称} 无 昨日收盘跌停，今日开盘卖出 的标的")
        else:
            可用资金, 持仓df, obj_list = qu.get_stock_持仓列表()
            可卖持仓df = 持仓df[持仓df['可卖数量'] > 0].copy()
            for index33, row33 in sell_df2.iterrows():
                qmt_code = row33['qmt_code']
                策略 = row33['策略']
                name = qu.get_name_by_qmtcode(qmt_code)
                tmpdf = 可卖持仓df[可卖持仓df['qmt_code'] == qmt_code].copy()
                if len(tmpdf):
                    tmpdata = tmpdf.iloc[0]
                    可卖数量 = tmpdata['可卖数量']
                    qu.sell_stock_he(ContextInfo, qmt_code, name, 可卖数量, 策略名称)  # 核按钮卖出
                    save_or_update_by_sql("UPDATE " + bsea_sell_table_t + " SET status=0 WHERE qmt_code='" + qmt_code + "' AND dtime='" + str(curr_date) + "' AND 策略='" + 策略 + "'")

    if curr_time > '09:25:20' and curr_time < '09:30:00':  # 买入
        # 读配置表，是否准备好买入数据
        is_prepared = is_竞价开关_prepared()
        if is_prepared:
            # 读取买入表
            df = get_df_from_table("SELECT * FROM " + bsea_buy_table_t + " WHERE dtime='" + curr_date + "' AND status=1 ORDER BY 策略 ASC, 推荐理由 ASC")
            print(df)
            if len(df) == 0:
                print(策略名称 + " " + curr_date + " 今日无xg结果！！！")
            else:
                for index, row in df.iterrows():
                    name = row['name']
                    qmt_code = row['qmt_code']  # 如'600000.SH'
                    策略 = row['策略']
                    买入价格 = row['买入价格']
                    买入股数 = row['买入股数']

                    qu.buy_stock(ContextInfo, qmt_code, name, 买入价格, 买入股数, 策略)
                    save_or_update_by_sql("UPDATE " + bsea_buy_table_t + " SET status=0 WHERE qmt_code='" + qmt_code + "' AND dtime='" + str(curr_date) + "' AND 策略='" + 策略 + "'")

        # 检查一字板开盘，写入全局变量
        if len(g_一字板_df) == 0:
            sell_df = get_sell_infos()
            for index, row in sell_df.iterrows():
                code = row['code']
                qmt_code = row['qmt_code']
                name = row['name']
                pre_close = row['pre_close']
                一字板, 涨停价 = qu.is_当天一字板_by_qmt(ContextInfo, qmt_code, pre_close)
                if 一字板:
                    print(策略名称 + " " + code + " 开盘顶一字板（如一直未开板就持有，开板就砸盘）")
                    g_一字板_df = g_一字板_df.append({'code': code, 'name': name, '涨停价': 涨停价}, ignore_index=True)  #

    if (curr_time >= '09:30:00' and curr_time < '11:33:00') or (curr_time >= '12:57:00' and curr_time < '15:03:00'):  # 卖出
        # 当前持仓查询
        可用资金, 持仓df, obj_list = qu.get_stock_持仓列表()
        可卖持仓df = 持仓df[持仓df['可卖数量'] > 0].copy()

        # 查询卖出表
        sell_df = get_sell_infos()
        print(sell_df)

        for index, row in sell_df.iterrows():
            code = row['code']
            qmt_code = row['qmt_code']
            策略 = row['策略']
            name = row['name']
            pre_close = row['pre_close']

            # 计算当日涨停、跌停价
            当日涨停价, 当日跌停价 = qu.get_涨停_跌停价_by_qmt(ContextInfo, qmt_code)
            print(f"{策略名称} {code}[{name}], pre_close: {pre_close}, 涨停价: {当日涨停价}, 跌停价: {当日跌停价}")
            tmpdf = 可卖持仓df[可卖持仓df['code'] == code].copy()
            if len(tmpdf) > 0:
                tmpdata = tmpdf.iloc[0]
                当前价 = tmpdata['当前价']
                可卖数量 = tmpdata['可卖数量']
                当日涨幅 = 100 * (当前价 - pre_close) / pre_close
                print(f"{策略名称} {code}[{name}] 当前价: {fmt_float2str(当前价)}, 当前涨幅: {fmt_float2str(当日涨幅)}")

                if 可卖数量 > 0:
                    # 查看是否一字板的情况
                    is_开盘一字板 = False
                    if len(g_一字板_df) > 0:
                        一字板tmpdf = g_一字板_df[g_一字板_df['code'] == code]
                        if len(一字板tmpdf) > 0:  # 开盘一字板的case
                            is_开盘一字板 = True
                            一字板tmpdata = 一字板tmpdf.iloc[0]
                            涨停价 = 一字板tmpdata['涨停价']
                            if 当前价 < 涨停价:  # 破板，立即挂跌停价卖出
                                qu.sell_stock(ContextInfo, qmt_code, name, 当日跌停价, 可卖数量, 策略)

                    if not is_开盘一字板:
                        if curr_time > '14:56:00':
                            # 先撤单
                            qu.cancel_all_order(ContextInfo, cst.account, 策略)
                            if curr_time > '14:58:00':
                                qu.sell_stock(ContextInfo, qmt_code, name, 当日跌停价, 可卖数量, 策略)  # 挂竞价单, 直接挂跌停价卖，最终会以收盘价成交
                        else:
                            if 可卖数量 > 0:
                                if 当前价 >= 当日涨停价 - 0.01:  # 摸涨停的情况
                                    qu.sell_stock(ContextInfo, qmt_code, name, (当日涨停价 - 0.01), 可卖数量, 策略)  # 摸涨停价卖出
                                elif 当前价 <= 当日跌停价 + 0.01:  # 摸跌停的情况
                                    qu.sell_stock(ContextInfo, qmt_code, name, 当日跌停价, 可卖数量, 策略)  # 摸跌停价卖出

    if (curr_time >= '09:59:40' and curr_time < '10:02:00'):  # 新股_新债_申购
        qu.新股_新债_申购(ContextInfo)

    if (curr_time >= '11:33:00' and curr_time < '12:57:00'):
        print(f"{策略名称} 中午已收盘, sleep 15s")
        time.sleep(15)

    if (curr_time >= '15:03:00' and curr_time < '15:33:00'):  # 全量资金all in国债逆回购1天期, 有防重复下单功能，实际钱不够也不可能重复买入。逆回购交易时间延长到15:30
        qu.国债逆回购(ContextInfo, cst.account)
        print(f"{策略名称} 已收盘, sleep 30s")
        time.sleep(30)

    if curr_time >= '15:33:00':  # 已收盘
        print(f"{策略名称} 已收盘, sleep 300s")
        time.sleep(600)


def init(ContextInfo):
    log_and_send_im(f"------$$$$$$ {get_curr_date()} {get_curr_time()} {策略名称} 已启动init")
    pass_qmt_funcs()

    ContextInfo.set_account(cst.account)
    timer_startTime = get_curr_date() + "09:20:10"

    ContextInfo.run_time("timerHandler", "3nSecond", timer_startTime)


def handlebar(ContextInfo):
    print(f'{策略名称} 这是 handlebar 中的 say hi~~~')

    d = ContextInfo.barpos
    realtime = ContextInfo.get_bar_timetag(d)
    nowdate = timetag_to_datetime(realtime, '%Y-%m-%d %H:%M:%S')
    print(nowdate)

    global g_countdown_latch
    g_countdown_latch -= 1
    if g_countdown_latch <= 0:
        g_countdown_latch = 8
        可用资金, 持仓df, obj_list = qu.get_stock_持仓列表()


def get_sell_infos():
    """ 查询卖出表 """
    sql2 = "SELECT * FROM " + bsea_sell_table_t + " WHERE sell_dtime='" + get_curr_date() + "' AND status=1 ORDER BY 策略 ASC, 子策略 ASC"
    sell_df = get_df_from_table(sql2)
    return sell_df


def is_竞价开关_prepared():
    """ 读配置表，是否准备好买入数据 """
    is_prepared = False

    conf_sql = "SELECT * FROM xyy_config WHERE conf='竞价自动下单'"
    conf_df = get_df_from_table(conf_sql)
    if len(conf_df) == 0:
        log_and_send_im(f"{策略名称} 无竞价自动下单配置，请检查xyy_config表对应的配置项")
        is_prepared = False
    else:
        conf_data = conf_df.iloc[0]
        if conf_data['val'] != '1':
            log_and_send_im(f"{策略名称} 竞价配置数据未准备好，请等待xg程序将'竞价自动下单'对应的val值设置为1...")
            is_prepared = False
        else:
            is_prepared = True
    return is_prepared


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
