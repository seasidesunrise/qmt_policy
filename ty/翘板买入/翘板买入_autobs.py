# encoding:gbk

# ����һ�����Խ��ף���������ͣ�ⵥ����2�ڣ�30���ڵ�ͣ�ɽ�����1�ڣ��ⵥ���С��5000w������

"""
�̰��������

"""

import bsea_utils.bsea_xyy_qmt_util as qu
from bsea_utils.bsea_xyy_util import *

cst.webhook = 'https://open.feishu.cn/open-apis/bot/v2/hook/6adbb16f-265f-4a92-bac8-b703b36c7448'

�������� = '�̰�����'
# buy_table = 'bsea_buy_info'
# sell_table = 'bsea_sell_info'

############  �˹�ָ�����ֿ�ʼ ###############

g_code_set = []  # ��Ʊ��ģ���Ҫ��һ��'�̰�����' ���Զ����飬��Ϊ��Ʊ��

g_�����ܽ�� = 10 * 10000  # �ò������ʹ�õ��ʽ�������λ��Ԫ
g_��֧��Ʊ���ʹ�ý�� = 5 * 10000  # ��֧��Ʊ�����λ����λ��Ԫ

g_������� = 30  # 30��
g_��ʼ��ͣ�ⵥ��� = 1 * 10000 * 10000  # ��ʼ�ⵥ1�ڣ���λ��Ԫ
g_��������ⵥ��� = 0.5 * 10000 * 10000  # �ⵥ���ٵ�����ʱ���������룬��λ��Ԫ

############  �˹�ָ�����ֽ��� ###############

g_countdown_latch = 8
g_prepare_df = g_final_df = pd.DataFrame()
g_�����¹��ĵ�_set = set()
g_sell_ί�е�_num = 0
g_code_set = set()


def recheck_prepare_stocks(ContextInfo):
    print(f'------$$$$$$ {��������} timerHandler��ʱ��' + get_curr_date() + " " + get_curr_time())
    global g_code_set
    global g_prepare_df
    g_prepare_df = pd.DataFrame()
    g_code_set = set()

    s = ContextInfo.get_stock_list_in_sector('�̰�����')
    print(f"{��������} �̰�������ɷֹɣ�" + str(s))
    g_code_set = set(s)
    print(f"{��������} g_code_set: {g_code_set}")
    print(f"{��������} g_code_list: {list(g_code_set)}")

    zuotian_date = get_zuotian_date()

    df_all = ContextInfo.get_market_data(fields=['volume', 'amount', 'open', 'high', 'low', 'close'], stock_code=list(g_code_set), period='1d', dividend_type='front', count=2)
    print(df_all)
    hq_all_dict = {}

    for qmt_code in g_code_set:
        name = qu.get_name_by_qmtcode(ContextInfo, qmt_code)
        ��ͣ��, ��ͣ�� = qu.get_��ͣ_��ͣ��_by_qmt(ContextInfo, qmt_code)

        df = df_all[qmt_code].copy()
        pre1k_data = df.iloc[-2]
        pre_close = pre1k_data['close']  # ����pre1k���̼�

        �����̰�����Ԥ������ = True
        print(f"{��������} {qmt_code}[{name}] �����̰�����Ԥ����������ʼ���������룬 ��ͣ��: {fmt_float2str(��ͣ��)}")
        g_prepare_df = g_prepare_df.append({'qmt_code': qmt_code, '�����̰�����Ԥ������': �����̰�����Ԥ������, '��ͣ��': ��ͣ��, 'name': name, 'pre_close': pre_close}, ignore_index=True)

    log_and_send_im(f"{��������}��Ԥ���ɳ�: {g_prepare_df}")


def init(ContextInfo):
    log_and_send_im(f"------$$$$$$ {get_curr_date()} {get_curr_time()} {��������} ����������init")
    pass_qmt_funcs()
    ContextInfo.set_account(cst.account)

    timer_startTime = get_curr_date() + "00:05:10"
    ContextInfo.run_time("recheck_prepare_stocks", "60nSecond", timer_startTime)


def handlebar(ContextInfo):
    print('{��������} ���� handlebar �е� 3��һ�ε�tick ~~~')

    global g_prepare_df
    global g_final_df
    global g_��֧��Ʊ���ʹ�ý��
    global g_�������, g_��ʼ��ͣ�ⵥ���, g_�����ⵥ���
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

    df3 = ContextInfo.get_full_tick(stock_code=g_prepare_df['qmt_code'].tolist())
    for index2, row2 in g_prepare_df.iterrows():
        qmt_code = row2['qmt_code']
        code = qmt_code[:6]
        pre_close = row2['pre_close']
        name = row2['name']
        ��ͣ�� = row2['��ͣ��']

        curr_data = df3.get(qmt_code)

        �����嵵 = curr_data['askPrice']
        ��һ�۸� = �����嵵[0]
        ����嵵 = curr_data['bidPrice']
        �����嵵 = curr_data['askVol']
        ��һ���� = �����嵵[0]
        �����嵵 = curr_data['bidVol']
        ��һ��� = ��һ�۸� * ��һ����
        �ɽ��� = curr_data['amount']
        �ɽ��� = curr_data['volume']
        print(f"��һ�۸�: {��һ�۸�}, ��һ����: {��һ����}, ��һ���: {��һ���}, �ɽ���: {�ɽ��� / 10000 / 10000}��, �ɽ���:{�ɽ��� / 100}��")

        close = curr_data['lastPrice']
        if close <= ��ͣ�� and ��һ��� >= g_��ʼ��ͣ�ⵥ���:  # ��ͣ���ҷⵥ����2��
            g_final_df = g_final_df.append({'qmt_code': qmt_code, '��ʼ�����һ�ⵥ': ��һ���, '��ʼ��سɽ���': �ɽ���, '��ʼ��سɽ���': �ɽ���, '��ʼ���ʱ��': time.time(), '��ʼ���ʱ��_dt': get_curr_time()})
            log_and_send_im(f"{qmt_code}[{name}] ��ͣ���ⵥ������ {g_��ʼ��ͣ�ⵥ��� / 10000 / 10000}�ڣ������ض���...")
            print(g_final_df)
            return

        if len(g_final_df) > 0:
            curr_final_df = g_final_df[g_final_df['qmt_code'] == qmt_code].copy()
            if len(curr_final_df) > 0:
                curr_final_data = curr_final_df.iloc[0]
                ��ʼ���ʱ�� = curr_final_data['��ʼ���ʱ��']
                ��ʼ��سɽ��� = curr_final_data['��ʼ��سɽ���']
                ��ǰʱ�� = time.time()
                if (��ǰʱ�� - ��ʼ���ʱ�� <= g_�������) and ((�ɽ��� - ��ʼ��سɽ���) > g_��������ⵥ���) and ��һ��� < 5000 * 10000:
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
