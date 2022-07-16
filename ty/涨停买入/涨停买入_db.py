# encoding:gbk


"""
 ����ͣ���룬ը�����ͣ����


 ������
    1���ⵥ����С��1000�򣬳���
    2��

:Author:  ��ң��
:Create:  2022/7/15$ 21:37$
"""

import bsea_utils.bsea_xyy_qmt_util as qu
from bsea_utils.bsea_xyy_util import *

�������� = '��ͣ����'
table_t = 'bsea_��ͣ����'

############  �˹�ָ�����ֿ�ʼ ###############

g_�����ܽ�� = 10 * 10000  # �ò������ʹ�õ��ʽ�������λ��Ԫ
g_��֧��Ʊ���ʹ�ý�� = 5 * 10000  # ��֧��Ʊ�����λ����λ��Ԫ

############  �˹�ָ�����ֽ��� ###############


g_countdown_latch = 8
g_prepare_df = pd.DataFrame(columns=['qmt_code', 'name', '��ͣ��', '��ͣ��', 'pre_close', '����ը��ط�����', '��������С�ڶ�����', '���ð�������С�ڶ�����ѡ��'])
g_final_df = pd.DataFrame(columns=['qmt_code', 'name'])
g_�����¹��ĵ�_set = set()


def recheck_prepare_stocks(ContextInfo):
    curr_time = get_curr_time()
    print(f'------$$$$$$ {��������} timerHandler��ʱ��' + get_curr_date() + " " + curr_time)

    global g_prepare_df

    if not check_is_����_or_�ٽ�(curr_time):
        # todo: ���� g_prepare_df �е���������
        print(f"{��������} ��ǰʱ�䲻�ڽ����У� {curr_time}")
        return

    sql_all_��� = "SELECT * FROM " + table_t + " WHERE status='1' AND account_nick='" + str(cst.account_nick) + "'"
    all_df = get_df_from_table(sql_all_���)
    if len(all_df) == 0:
        print(f"{��������} ��Ч���Ϊ�գ�����")

    df_all = ContextInfo.get_market_data(fields=['volume', 'amount', 'open', 'high', 'low', 'close'], stock_code=all_df['qmt_code'].tolist(), period='1d', dividend_type='front', count=2)
    print(df_all)
    for index, row in all_df.iterrows():
        qmt_code = row['qmt_code']
        ��������С�ڶ����� = get_num_by_numfield(row, '��������С�ڶ�����')
        ���ð�������С�ڶ�����ѡ�� = get_num_by_numfield(row, '���ð�������С�ڶ�����ѡ��')
        ����ը��ط����� = get_num_by_numfield(row, '����ը��ط�����')
        name = qu.get_name_by_qmtcode(ContextInfo, qmt_code)
        ��ͣ��, ��ͣ�� = qu.get_��ͣ_��ͣ��_by_qmt(ContextInfo, qmt_code)

        df = df_all[qmt_code].copy()
        pre1k_data = df.iloc[-2]
        pre_close = pre1k_data['close']  # ����pre1k���̼�

        print(f"{��������} {qmt_code}[{name}], ��ͣ��: {fmt_float2str(��ͣ��)}, ��ͣ��: {fmt_float2str(��ͣ��)}")
        g_prepare_df.loc[qmt_code] = {'qmt_code': qmt_code, 'name': name, '��ͣ��': ��ͣ��, '��ͣ��': ��ͣ��, 'pre_close': pre_close, '���ð�������С�ڶ�����ѡ��': ���ð�������С�ڶ�����ѡ��, '��������С�ڶ�����': ��������С�ڶ�����}

    print(f"{��������}��Ԥ���ɳ�: {g_prepare_df}")


def init(ContextInfo):
    log_and_send_im(f"------$$$$$$ {get_curr_date()} {get_curr_time()} {��������} ����������init")
    pass_qmt_funcs()
    ContextInfo.set_account(cst.account)

    timer_startTime = get_curr_date() + "06:05:10"
    ContextInfo.run_time("recheck_prepare_stocks", "10nSecond", timer_startTime)


def handlebar(ContextInfo):
    print('{��������} ���� handlebar �е� 3��һ�ε�tick ~~~')

    global g_prepare_df
    global g_final_df
    global g_�����¹��ĵ�_set
    d = ContextInfo.barpos
    realtime = ContextInfo.get_bar_timetag(d)
    nowdate = timetag_to_datetime(realtime, '%Y-%m-%d %H:%M:%S')

    global g_countdown_latch
    g_countdown_latch -= 1
    if g_countdown_latch <= 0:
        g_countdown_latch = 8
        �����ʽ�, �ֲ�df, obj_list = qu.get_stock_�ֲ��б�(cst.account)

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
        ��ͣ�� = row2['��ͣ��']
        ��ͣ�� = row2['��ͣ��']
        ��������С�ڶ����� = row2('��������С�ڶ�����')
        ���ð�������С�ڶ�����ѡ�� = row2('���ð�������С�ڶ�����ѡ��')
        ����ը��ط����� = row2('����ը��ط�����')
        curr_data = df3.get(qmt_code)
        if curr_data is None:
            print(f"get data err: {qmt_code}[{name}]")
            continue

        �����嵵 = curr_data['askPrice']
        ��һ�۸� = �����嵵[0]
        ����嵵 = curr_data['bidPrice']
        ��һ�۸� = ����嵵[0]
        �����嵵 = curr_data['askVol']
        ��һ���� = �����嵵[0]
        �����嵵 = curr_data['bidVol']
        ��һ���� = �����嵵[0]
        ��һ��� = ��һ�۸� * ��һ���� * 100 / 10000  # ��λ����
        ��һ��� = ��һ�۸� * ��һ���� * 100 / 10000  # ��λ����
        �ɽ��� = curr_data['amount'] / 10000  # ��λ����
        �ɽ��� = curr_data['volume']  # ��λ����

        small_flt = 1 / 10000 / 10000
        close = curr_data['lastPrice']
        close = round(close + small_flt, 2)
        ��ͣ�� = round(��ͣ�� - small_flt, 2)
        ��ͣ�� = round(��ͣ�� + small_flt, 2)

        print(f"{qmt_code}[{name}] ��ǰ��: {close}, ��ͣ��: {��ͣ��}, ��ͣ��: {��ͣ��},  ��һ�۸�: {��һ�۸�}, ��һ����: {��һ����}, ��һ���: {��һ���}, �ɽ���: {�ɽ���}��, �ɽ���:{�ɽ���}��, close <= ��ͣ��: {close >= ��ͣ��}")

        if close >= ��ͣ��:
            �Ƿ����� = False
            if len(g_final_df) > 0:
                if len(g_final_df[g_final_df['qmt_code'] == qmt_code]) > 0:  # ��ɾ��
                    �Ƿ����� = True
            if not �Ƿ�����:
                ����۸� = close
                ������� = int(g_��֧��Ʊ���ʹ�ý�� / ����۸� / 100) * 100
                ������� = 100  # todo������100��

                if ����ը��ط����� == 1:
                    df_all = ContextInfo.get_market_data(fields=['volume', 'amount', 'open', 'high', 'low', 'close'], stock_code=all_df['qmt_code'].tolist(), period='1d', dividend_type='front', count=2)
                    print(df_all)

                else:
                    if ���ð�������С�ڶ�����ѡ�� == 1:  # check ��һѡ��
                        if ��һ��� < ��������С�ڶ�����:
                            log_and_send_im(f"{qmt_code}[{name}] ����ͣ����һ���: {��һ���}, ��������С�� {��������С�ڶ�����}���µ�����")
                            qu.buy_stock(ContextInfo, qmt_code, name, ��ͣ��, �������, ��������)
                            g_final_df.loc[qmt_code] = {'qmt_code': qmt_code, 'name': name}
                    else:
                        log_and_send_im(f"{qmt_code}[{name}] ����ͣ����һ���: {��һ���}����ͣ���µ�����")
                        qu.buy_stock(ContextInfo, qmt_code, name, ��ͣ��, �������, ��������)
                        g_final_df.loc[qmt_code] = {'qmt_code': qmt_code, 'name': name}

            return


def pass_qmt_funcs():
    qu.passorder = passorder
    qu.get_trade_detail_data = get_trade_detail_data
    qu.get_new_purchase_limit = get_new_purchase_limit
    qu.get_ipo_data = get_ipo_data


def deal_callback(ContextInfo, dealInfo):
    """ ���˺ųɽ�״̬�б仯ʱ����ִ��������� """
    qu.deal_callback_func(dealInfo, ��������)


def stop(ContextInfo):
    qu.stop_policy(��������)
