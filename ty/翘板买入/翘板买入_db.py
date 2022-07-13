# encoding:gbk

# ����һ�����Խ��ף���������ͣ�ⵥ����2�ڣ�30���ڵ�ͣ�ɽ�����1�ڣ��ⵥ���С��5000w������

"""
�̰��������

"""

import bsea_utils.bsea_xyy_qmt_util as qu
from bsea_utils.bsea_xyy_util import *

cst.webhook = 'https://open.feishu.cn/open-apis/bot/v2/hook/6adbb16f-265f-4a92-bac8-b703b36c7448'

�������� = '�̰�����'
table_t = 'bsea_�̰�����'

############  �˹�ָ�����ֿ�ʼ ###############

g_�����ܽ�� = 10 * 10000  # �ò������ʹ�õ��ʽ�������λ��Ԫ
g_��֧��Ʊ���ʹ�ý�� = 5 * 10000  # ��֧��Ʊ�����λ����λ��Ԫ

############  �˹�ָ�����ֽ��� ###############

g_countdown_latch = 8
g_prepare_df = pd.DataFrame(columns=['qmt_code', '�����̰�����Ԥ������', '��ͣ��', 'name', 'pre_close', '�������', '��ʼ��ͣ�ⵥ���', '��������ⵥ���', '������������ٳɽ����'])
g_final_df = pd.DataFrame(columns=['qmt_code', '��ʼ�����һ�ⵥ', '��ʼ��سɽ���', '��ʼ��سɽ���', '��ʼ���ʱ��', '��ʼ���ʱ��_dt'])


def recheck_prepare_stocks(ContextInfo):
    curr_time = get_curr_time()
    print(f'------$$$$$$ {��������} timerHandler��ʱ��' + get_curr_date() + " " + curr_time)

    # if not check_is_����_or_�ٽ�(curr_time):
    #     print(f"{��������} ��ǰʱ�䲻�ڽ����У� {curr_time}")
    #     return

    sql_all_��� = "SELECT * FROM " + table_t + " WHERE status='1' AND account_nick='" + str(cst.account_nick) + "'"
    all_df = get_df_from_table(sql_all_���)
    if len(all_df) == 0:
        print(f"{��������} {table_t} ��Ч���Ϊ�գ�����")

    global g_prepare_df
    df_all = ContextInfo.get_market_data(fields=['volume', 'amount', 'open', 'high', 'low', 'close'], stock_code=all_df['qmt_code'].tolist(), period='1d', dividend_type='front', count=2)
    print(df_all)
    for index, row in all_df.iterrows():
        qmt_code = row['qmt_code']
        ������� = row['�������']
        ��ʼ��ͣ�ⵥ��� = row['��ʼ��ͣ�ⵥ���']
        ��������ⵥ��� = row['��������ⵥ���']
        ������������ٳɽ���� = row['������������ٳɽ����']
        name = qu.get_name_by_qmtcode(ContextInfo, qmt_code)
        ��ͣ��, ��ͣ�� = qu.get_��ͣ_��ͣ��_by_qmt(ContextInfo, qmt_code)

        df = df_all[qmt_code].copy()
        pre1k_data = df.iloc[-2]
        pre_close = pre1k_data['close']  # ����pre1k���̼�

        �����̰�����Ԥ������ = True
        print(f"{��������} {qmt_code}[{name}] �����̰�����Ԥ����������ʼ���������룬 ��ͣ��: {fmt_float2str(��ͣ��)}")
        g_prepare_df.loc[qmt_code] = {'qmt_code': qmt_code, '�����̰�����Ԥ������': �����̰�����Ԥ������, '��ͣ��': ��ͣ��, 'name': name, 'pre_close': pre_close, '�������': �������, '��ʼ��ͣ�ⵥ���': ��ʼ��ͣ�ⵥ���, '��������ⵥ���': ��������ⵥ���, '������������ٳɽ����': ������������ٳɽ����}

    print(f"{��������}��Ԥ���ɳ�: {g_prepare_df}")


def init(ContextInfo):
    log_and_send_im(f"------$$$$$$ {get_curr_date()} {get_curr_time()} {��������} ����������init")
    pass_qmt_funcs()
    ContextInfo.set_account(cst.account)

    timer_startTime = get_curr_date() + "00:05:10"
    ContextInfo.run_time("recheck_prepare_stocks", "15nSecond", timer_startTime)


def handlebar(ContextInfo):
    print('{��������} ���� handlebar �е� 3��һ�ε�tick ~~~')

    global g_prepare_df
    global g_final_df
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
        ������� = row2['�������']  # ��λ����
        ��ʼ��ͣ�ⵥ��� = row2['��ʼ��ͣ�ⵥ���']  # ��λ����Ԫ
        ��������ⵥ��� = row2['��������ⵥ���']  # ��λ����Ԫ
        ������������ٳɽ���� = row2['������������ٳɽ����']  # ��λ����Ԫ

        curr_data = df3.get(qmt_code)

        �����嵵 = curr_data['askPrice']
        ��һ�۸� = �����嵵[0]
        ����嵵 = curr_data['bidPrice']
        �����嵵 = curr_data['askVol']
        ��һ���� = �����嵵[0]
        �����嵵 = curr_data['bidVol']
        ��һ��� = ��һ�۸� * ��һ���� * 100 / 10000  # ��λ����
        �ɽ��� = curr_data['amount'] / 10000  # ��λ����
        �ɽ��� = curr_data['volume']  # ��λ����

        small_flt = 1 / 10000 / 10000
        close = curr_data['lastPrice']
        close = round(close + small_flt, 2)
        ��ͣ�� = round(��ͣ�� + small_flt, 2)

        print(f"{qmt_code}[{name}] ��ǰ��: {close}, ��ͣ��: {��ͣ��},  ��һ�۸�: {��һ�۸�}, ��һ����: {��һ����}, ��һ���: {��һ���}, �ɽ���: {�ɽ���}��, �ɽ���:{�ɽ��� }��, ��ʼ��ͣ�ⵥ��{��ʼ��ͣ�ⵥ���}��,  close <= ��ͣ��: {close <= ��ͣ��}")

        if close <= ��ͣ�� and ��һ��� >= ��ʼ��ͣ�ⵥ���:  # ��ͣ���ҷⵥ����2000��
            has_code = False
            if len(g_final_df) > 0:
                if len(g_final_df[g_final_df['qmt_code'] == qmt_code]) > 0:  # ��ɾ��
                    has_code = True
            if not has_code:
                g_final_df.loc[qmt_code] = {'qmt_code': qmt_code, '��ʼ�����һ�ⵥ': ��һ���, '��ʼ��سɽ���': �ɽ���, '��ʼ��سɽ���': �ɽ���, '��ʼ���ʱ��': time.time(), '��ʼ���ʱ��_dt': get_curr_time()}
                log_and_send_im(f"{qmt_code}[{name}] ��ͣ���ⵥ������ {��ʼ��ͣ�ⵥ��� }�򣬽����ض���...")
                print(g_final_df)
            return

        if len(g_final_df) > 0:
            curr_final_df = g_final_df[g_final_df['qmt_code'] == qmt_code].copy()
            if len(curr_final_df) > 0:
                curr_final_data = curr_final_df.iloc[0]
                ��ʼ���ʱ�� = curr_final_data['��ʼ���ʱ��']
                ��ʼ��سɽ��� = curr_final_data['��ʼ��سɽ���']
                ��ǰʱ�� = time.time()
                if (��ǰʱ�� - ��ʼ���ʱ�� <= �������) and ((�ɽ��� - ��ʼ��سɽ���) > ������������ٳɽ����) and (��һ��� <= ��������ⵥ���):
                    # ��������
                    ����۸� = pre_close * (100 - 9) / 100
                    ������� = int(g_��֧��Ʊ���ʹ�ý�� / ����۸� / 100) * 100
                    ������� = 100  # todo�������ڼ䣬ͳһ��100��
                    qu.buy_stock(ContextInfo, qmt_code, name, ����۸�, �������, ��������)
                    log_and_send_im(f"{qmt_code}[{name}] ��ͣ�ⵥ������٣��������룬���µ�")


def pass_qmt_funcs():
    qu.passorder = passorder
    qu.get_trade_detail_data = get_trade_detail_data
    qu.get_new_purchase_limit = get_new_purchase_limit
    qu.get_ipo_data = get_ipo_data


def stop(ContextInfo):
    log_and_send_im(f"------$$$$$$ {get_curr_date()} {get_curr_time()} {��������} ������ֹͣ��")
