# encoding:gbk

"""
˫���������������

"""

import talib

import bsea_utils.bsea_xyy_qmt_util as qu
from bsea_utils.bsea_xyy_util import *

�������� = '˫��������_period'
buy_table = 'bsea_buy_info'
sell_table = 'bsea_sell_info'

############  �˹�ָ�����ֿ�ʼ ###############

g_code_set = []  # ��Ʊ��ģ���Ҫ��һ��'˫��������' ���Զ����飬��Ϊ��Ʊ��

g_�����ܽ�� = 10 * 10000  # �ò������ʹ�õ��ʽ�������λ��Ԫ
g_��֧��Ʊ���ʹ�ý�� = 5 * 10000  # ��֧��Ʊ�����λ����λ��Ԫ

g_pre1k�µ��ٷֱ� = 5  # ��0-20֮�����ֵ������5�����ʾ�µ�5%�����ϣ����µ�����>=5%
g_pre2k�µ��ٷֱ� = 5  # ��0-20֮�����ֵ������5�����ʾ�µ�5%�����ϣ����µ�����>=5%
g_curr�µ���ֵ = 1  # ��0-20֮�����ֵ������3�����ʾ�µ�3%�����ϣ���ִ������

g_ֹӯ���� = 10  # ��0���ϵ���ֵ������10�����ʾ10%���Զ�ֹӯ
g_ֹ����� = 10  # ��0���ϵ���ֵ������10�����ʾ10%���Զ�ֹ��
g_����CCI���� = False  # cci
g_���óɽ��� = True  # �ɽ�����curr��pre1k��pre2k������ҪС��ma5�ɽ���
g_period = '1h'  # �������ڣ�period֧��5m, 15m, 30m, 1h, 1d, 1w, 1mon

############  �˹�ָ�����ֽ��� ###############

g_countdown_latch = 8
g_prepare_df = pd.DataFrame()  # 'code' code, '����˫������������': 1, 'pre_close': 8.5
g_�����¹��ĵ�_set = set()
g_sell_ί�е�_num = 0
g_code_set = set()


def recheck_prepare_stocks(ContextInfo):
    print(f'------$$$$$$ {��������} timerHandler��ʱ��' + get_curr_date() + " " + get_curr_time())
    global g_code_set
    global g_prepare_df
    global g_period
    g_prepare_df = pd.DataFrame()
    g_code_set = set()

    s = ContextInfo.get_stock_list_in_sector('˫��������')
    print(f"{��������} ˫����������ɷֹɣ�" + str(s))
    g_code_set = set(s)
    print(f"{��������} g_code_set: {g_code_set}")
    print(f"{��������} g_code_list: {list(g_code_set)}")

    zuotian_date = get_zuotian_date()

    df_all = ContextInfo.get_market_data(fields=['volume', 'amount', 'open', 'high', 'low', 'close'], stock_code=list(g_code_set), period=g_period, dividend_type='front', count=10)
    print(df_all)
    hq_all_dict = {}

    for qmt_code in g_code_set:
        name = qu.get_name_by_qmtcode(ContextInfo, qmt_code)
        cnt_ma5_lower = 3
        df = df_all[qmt_code].copy()
        if len(df) <= cnt_ma5_lower:
            print(f"{��������} ȡ����k������̫�٣�С�ڵ���{cnt_ma5_lower}������")
            continue
        df['ma5'] = talib.MA(df['close'], 5)
        df['pre_close'] = df['close'].shift(1)
        df['�Ƿ�'] = 100 * (df['close'] - df['pre_close']) / df['pre_close']
        df['close����ma5'] = df.apply(lambda x: 1 if (x['close'] <= x['ma5']) else 0, axis=1)
        print(df)
        hq_all_dict.update({"code": qmt_code, "df": df})

        pre1k_data = df.iloc[-2]
        pre2k_data = df.iloc[-3]
        if pre1k_data['�Ƿ�'] <= -g_pre1k�µ��ٷֱ� < 0 and pre2k_data['�Ƿ�'] <= -g_pre2k�µ��ٷֱ� < 0:
            ����˫��������Ԥ������ = True
            pre_close = pre1k_data['close']  # ����pre1k���̼�
            log_and_send_im(f"{��������} {qmt_code}[{name}] ����˫��������Ԥ����������ʼ���������룬 pre_close: {fmt_float2str(pre_close)}")
            g_prepare_df = g_prepare_df.append({'qmt_code': qmt_code, '����˫��������Ԥ������': ����˫��������Ԥ������, 'pre_close': pre_close, 'name': name}, ignore_index=True)

    print(f"{��������} ��һ���ڶ�������������Ԥ���ɳ�: {g_prepare_df}")


def init(ContextInfo):
    log_and_send_im(f"------$$$$$$ {get_curr_date()} {get_curr_time()} {��������} ����������init")
    pass_qmt_funcs()
    ContextInfo.set_account(cst.account)

    timer_startTime = get_curr_date() + "00:05:10"
    ContextInfo.run_time("recheck_prepare_stocks", "20nSecond", timer_startTime)


def handlebar(ContextInfo):
    print('{��������} ���� handlebar �е� 3��һ�ε�tick ~~~')

    global g_prepare_df
    global g_period
    d = ContextInfo.barpos
    realtime = ContextInfo.get_bar_timetag(d)
    nowdate = timetag_to_datetime(realtime, '%Y-%m-%d %H:%M:%S')

    global g_countdown_latch
    g_countdown_latch -= 1
    if g_countdown_latch <= 0:
        g_countdown_latch = 8
        �����ʽ�, �ֲ�df, obj_list = qu.get_stock_�ֲ��б�(cst.account)

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
            �Ƿ� = get_�Ƿ�(close, pre_close)
            if �Ƿ� <= -g_curr�µ���ֵ < 0:
                # �ж��Ƿ�������
                dtime_curr = get_curr_date()

                select_sql = "SELECT * FROM " + buy_table + " WHERE code='" + qmt_code[:6] + "' AND dtime='" + dtime_curr + "' AND ����='" + �������� + "' AND status=1"
                select_df = get_df_from_table(select_sql)
                if len(select_df) > 0:  # ���¹���
                    print(f"{��������} ���¹���������: {select_df.iloc[0]}")
                    continue

                # ����CCI
                global g_����CCI����
                global g_���óɽ���
                if g_���óɽ��� or g_����CCI����:
                    df32 = ContextInfo.get_market_data(fields=['volume', 'amount', 'open', 'high', 'low', 'close'], stock_code=[qmt_code], period=g_period, dividend_type='front', count=21)
                    if len(df32) == 0:
                        log_and_send_im(f"{��������} ��ȡcci��������ָ������Դ����, ����ϵqmt��������״̬")
                        continue

                    if g_����CCI����:
                        cci_timeperiode = 14
                        if len(df32) == 0:
                            log_and_send_im(f"{��������} ��ȡcciָ������Դ����, ����ϵqmt��������״̬")
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
                            print(f"{��������} CCIָ��Ϊ: {curr_data_cci}, ����������3Kһ���һ��󣬺���")
                            continue

                    if g_���óɽ���:
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
                            print(f"{��������} vol������ָ��Ϊ: {curr_data}, ����������3K����5������������")
                            continue

                �����ʽ� = qu.get_�����ʽ�()
                �����ʽ� = min(g_��֧��Ʊ���ʹ�ý��, �����ʽ�)
                ����۸� = close  # ���������Ϊ��ǰ��
                ������� = 100 * int(�����ʽ� / ����۸� / 100)
                ������� = 100  # todo�������ã������������100��

                qu.he_buy_stock(ContextInfo, qmt_code, name, �������, ��������)
                log_and_send_im(f"{��������} {name}[{qmt_code}]�ﵽ�������µ���ֵ{g_curr�µ���ֵ}%��ί�����룬�µ����: {�����ʽ�}, ί�м۸񣺺˰�ť����, ��������� {�������}")

                # insert���������������־��
                ����ʱ�� = dtime_curr + " " + get_curr_time()
                insert_sql_buy = "REPLACE INTO " + buy_table + "(code, qmt_code, dtime, name, status, ����, ����۸�, �������, ������, �µ�ʱ��, period, account_nick, lastmodified) values ('" \
                                 + code + "', '" + qmt_code + "', '" + dtime_curr + "', '" + name + "', 1, '" + �������� + "', '" \
                                 + str(����۸�) + "', '" + str(�������) + "', '" + str(������� * ����۸�) + "', '" + dtime_curr + "', '" \
                                 + g_period + "', '" + cst.account_nick + "', '" + get_lastmodified() + "')"
                save_or_update_by_sql(insert_sql_buy)

                # insert����������
                insert_sql_sell = "REPLACE INTO " + sell_table + "(qmt_code, name, ����ʱ��, �����������, �Ƿ�����, period, account_nick, lastmodified) values ('" + \
                                  code + "', '" + name + "', '" + ����ʱ�� + "', '" + �������� + "', '0', '" + g_period + "', '" + cst.account_nick + "', '" + get_lastmodified() + "')"
                save_or_update_by_sql(insert_sql_sell)

    # ��������߼�
    sql = "SELECT * FROM " + sell_table + " WHERE �����������='" + �������� + "' AND ����ʱ��<'" + get_curr_date() + "' AND �Ƿ�����=0 AND account_nick='" + cst.account_nick + "' ORDER BY qmt_code ASC"
    sell_df = get_df_from_table(sql)
    if len(sell_df) == 0:
        print("{��������} ����������ģ�����")
        return

    �����ʽ�, �ֲ�df, obj_list = qu.get_stock_�ֲ��б�()
    for index4, row4 in sell_df.iterrows():
        qmt_code = row4['qmt_code']
        �ֲ�df2 = �ֲ�df[�ֲ�df['qmt_code'] == qmt_code].copy()
        if len(�ֲ�df2) == 0:
            print(f"{��������} �ֲ����Ѿ��޴˱��: {qmt_code}")
            update_sql = "UPDATE " + sell_table + " SET �Ƿ�����=1 WHERE qmt_code='" + qmt_code + "'"
            save_or_update_by_sql(update_sql)
            continue
        data0 = �ֲ�df2.iloc[0]
        if data0['ӯ������'] >= g_ֹӯ���� / 100:
            # �µ�����
            �����۸� = data0['��ǰ��']
            �������� = data0['��������']
            ��ǰ�ֲ��� = data0['��ǰ�ֲ���']
            if �������� <= 0:
                log_and_send_im(f"{��������} {qmt_code}��ǰ�ֲ�����{��ǰ�ֲ���}, ��������Ϊ: {��������}, �޷�����������")
            else:
                qu.he_sell_stock(ContextInfo, code, name, ��������, ��������)  # �ŵ�ǰ��ȥ����'�Ƿ�����'=1


def deal_callback(ContextInfo, dealInfo):
    """ ���˺ųɽ�״̬�б仯ʱ����ִ��������� """
    qu.deal_callback_func(dealInfo, ��������)


def pass_qmt_funcs():
    qu.passorder = passorder
    qu.get_trade_detail_data = get_trade_detail_data
    qu.get_new_purchase_limit = get_new_purchase_limit
    qu.get_ipo_data = get_ipo_data


def stop(ContextInfo):
    log_and_send_im(f"------$$$$$$ {get_curr_date()} {get_curr_time()} {��������} ������ֹͣ��")
