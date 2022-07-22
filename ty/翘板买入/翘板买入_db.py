# encoding:gbk

# �̰��������
# ����һ�����Խ��ף���������ͣ�ⵥ����2�ڣ�30���ڵ�ͣ�ɽ�����1�ڣ��ⵥ���С��5000w������
# �����  ����ʼ��ͣ�ⵥ�� ���ó�0�� �Ҽ������Ϊ0�����ʱֱ���ж���һ�ⵥ��С�� ����������ⵥ��  ��ί�����룻������������Ϊ0�����ر���30���ڵ�ͣ�ɽ��1�ڣ��ⵥ���С��5000w���������߼���


# ���Ͼ����ڼ������ų� ��һ��׼
# ��������ⵥ��Ϊ0�����
#
#

import bsea_utils.bsea_xyy_qmt_util as qu
from bsea_utils.bsea_xyy_util import *

# cst.webhook = 'https://open.feishu.cn/open-apis/bot/v2/hook/6adbb16f-265f-4a92-bac8-b703b36c7448'

�������� = '�̰�����'
table_t = 'bsea_�̰�����'
g_data = {}

############  �˹�ָ�����ֿ�ʼ ###############

g_�̶�����100�� = 1  # ֵΪ0ʱ�������ݿ�����Ϊ׼; Ϊ1ʱ���������ԣ����̶�100�ɽ��ף��������ԣ��ƴ����ʶ����200�ɣ���������

############  �˹�ָ�����ֽ��� ###############

g_countdown_latch = 8
g_prepare_df = pd.DataFrame(columns=['qmt_code', 'name', '�����ʽ�', '��ͣ��', 'pre_close', '�������', '��ʼ��ͣ�ⵥ���', '��������ⵥ���', '������������ٳɽ����'])
g_�����¹��ĵ�_set = set()


def recheck_prepare_stocks(ContextInfo):
    curr_time = get_curr_time()
    print(f'------$$$$$$ {��������} timerHandler��ʱ��' + get_curr_date() + " " + curr_time)

    if not check_is_����_or_�ٽ�(curr_time):
        print(f"{��������} ��ǰʱ�䲻�ڽ����У� {curr_time}")
        return

    sql_all_��� = "SELECT * FROM " + table_t + " WHERE status='1' AND account_nick='" + str(cst.account_nick) + "'"
    all_df = get_df_from_table(sql_all_���)
    if len(all_df) == 0:
        print(f"{��������} {table_t} ��Ч���Ϊ�գ�����")
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
        �����ʽ� = row['�����ʽ�']
        ������� = row['�������']
        ��ʼ��ͣ�ⵥ��� = row['��ʼ��ͣ�ⵥ���']
        ��������ⵥ��� = row['��������ⵥ���']
        ������������ٳɽ���� = row['������������ٳɽ����']
        name = qu.get_name_by_qmtcode(ContextInfo, qmt_code)
        ��ͣ��, ��ͣ�� = qu.get_��ͣ_��ͣ��_by_qmt(ContextInfo, qmt_code)

        df = df_all[qmt_code].copy()
        pre1k_data = df.iloc[-2]
        pre_close = pre1k_data['close']  # ����pre1k���̼�

        print(f"{��������} {qmt_code}[{name}]�� ��ͣ��: {fmt_float2str(��ͣ��)}")
        g_prepare_df.loc[qmt_code] = {'qmt_code': qmt_code, 'name': name, '�����ʽ�': �����ʽ�, '��ͣ��': ��ͣ��, 'pre_close': pre_close, '�������': �������, '��ʼ��ͣ�ⵥ���': ��ʼ��ͣ�ⵥ���, '��������ⵥ���': ��������ⵥ���, '������������ٳɽ����': ������������ٳɽ����}

    print(f"{��������}��Ԥ���ɳ�: {g_prepare_df}")


def init(ContextInfo):
    global g_�̶�����100��
    �̶�����100��_msg = "" if not g_�̶�����100�� else "->100��ģʽ!!"
    log_and_send_im(f"------$$$$$$ {get_curr_date()}  {get_curr_time()}  {��������}  {�̶�����100��_msg} ����������init")

    pass_qmt_funcs()
    ContextInfo.set_account(cst.account)

    timer_startTime = get_curr_date() + "00:05:10"
    ContextInfo.run_time("recheck_prepare_stocks", "15nSecond", timer_startTime)


def is_�����̰�ʱ��(curr_time):
    """ 9��33�ֵ�β��14��54��Ϊ�̰幤��ʱ�䣬����ʱ����� """
    if curr_time >= '09:33:00' and curr_time < '14:54:00':
        return True
    else:
        print(f"{��������} ��ǰʱ�� {curr_time} �����̰幤��ʱ��[09:33 ~ 14:54]")
        return False


def handlebar(ContextInfo):
    print('{��������} ���� handlebar �е� 3��һ�ε�tick ~~~')

    curr_date = get_curr_date()
    curr_time = get_curr_time()
    curr_dtime = curr_date + " " + curr_time

    if not is_�����̰�ʱ��(curr_time):
        return

    global g_prepare_df
    global g_�����¹��ĵ�_set
    global g_countdown_latch
    global g_data

    g_countdown_latch -= 1
    if g_countdown_latch <= 0:
        g_countdown_latch = 8
        �����ʽ�, �ֲ�df, obj_list = qu.get_stock_�ֲ��б�(cst.account)

    if len(g_prepare_df) == 0:
        return

    df3 = ContextInfo.get_full_tick(stock_code=g_prepare_df['qmt_code'].tolist())

    for index2, row2 in g_prepare_df.iterrows():
        qmt_code = row2['qmt_code']
        code = qmt_code[:6]
        pre_close = row2['pre_close']
        �����ʽ� = row2['�����ʽ�']
        name = row2['name']
        ��ͣ�� = row2['��ͣ��']
        ������� = row2['�������']  # ��λ����
        ��ʼ��ͣ�ⵥ��� = row2['��ʼ��ͣ�ⵥ���']  # ��λ����Ԫ
        ��������ⵥ��� = row2['��������ⵥ���']  # ��λ����Ԫ
        ������������ٳɽ���� = row2['������������ٳɽ����']  # ��λ����Ԫ
        ������С���� = qu.get_������С����_by_qmt_code(qmt_code)

        curr_data = df3.get(qmt_code)
        if curr_data is None:
            print(f"get data err: {qmt_code}[{name}]")
            continue

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

        print(f"{qmt_code}[{name}] ��ǰ��: {close}, ��ͣ��: {��ͣ��},  ��һ�۸�: {��һ�۸�}, ��һ����: {��һ����}, ��һ���: {��һ���}, �ɽ���: {�ɽ���}��, �ɽ���:{�ɽ���}��, ��ʼ��ͣ�ⵥ��{��ʼ��ͣ�ⵥ���}��,  close <= ��ͣ��: {close <= ��ͣ��}")

        g_final_df = g_data.get(qmt_code)
        if g_final_df is None:
            g_final_df = pd.DataFrame(columns=['qmt_code', 'name', 'curr_time', '��һ���', '��ǰ�ܳɽ���', '��ǰ�ܳɽ���', '��ʼ�����һ�ⵥ', '��ʼ��سɽ���', '��ʼ��سɽ���', '��ʼ���ʱ��', '��ʼ���ʱ��_dt'])
        if ��ʼ��ͣ�ⵥ��� == 0:
            if ������� == 0:  # �������Ϊ0�� ֻ��'��������ⵥ���'�����Ƿ�����
                if ��һ��� <= ��������ⵥ���:
                    # ��������
                    if qmt_code not in g_�����¹��ĵ�_set:
                        g_�����¹��ĵ�_set.add(qmt_code)
                        ����۸� = pre_close * (100 - 9) / 100
                        ������� = int(�����ʽ� / ����۸� / 100) * 100
                        if g_�̶�����100��:
                            ������� = ������С����

                        qu.buy_stock(ContextInfo, qmt_code, name, ����۸�, �������, ��������)
                        log_and_send_im(f"{qmt_code}[{name}] ��ʼ��ͣ�ⵥ���=0�� �ҵ�ͣ�ⵥС�ڴ�������ⵥ���������룬���µ�, ����۸�{fmt_float2str(����۸�)}, �������: {�������}")
                    else:
                        print(f"{qmt_code}[{name}] �������¹���������")
            else:
                # ֻ��'��ʼ��ͣ�ⵥ���'�ֶ�Ϊ0���������������ж�
                if close <= ��ͣ��:  # ��ͣ
                    log_and_send_im(f"{qmt_code}[{name}] ��ͣ���ⵥ������ {��ʼ��ͣ�ⵥ���}�򣬽����ض���...")
                    g_final_df.loc[curr_time] = {'qmt_code': qmt_code, 'name': name, 'curr_time': curr_time, '��һ���': ��һ���, '��ǰ�ܳɽ���': �ɽ���, '��ǰ�ܳɽ���': �ɽ���}
                    g_data[qmt_code] = g_final_df

                    cnt = ������� / 3
                    if len(g_final_df) > cnt:
                        data_start = g_final_df.iloc[cnt - 1]
                        data_curr = g_final_df.iloc[0]
                        if data_curr['��ǰ�ܳɽ���'] - data_start['��ǰ�ܳɽ���'] >= ������������ٳɽ����:  # ��������
                            if qmt_code not in g_�����¹��ĵ�_set:
                                g_�����¹��ĵ�_set.add(qmt_code)
                                ����۸� = pre_close * (100 - 9) / 100
                                ������� = int(�����ʽ� / ����۸� / 100) * 100
                                if g_�̶�����100��:
                                    ������� = ������С����

                                qu.buy_stock(ContextInfo, qmt_code, name, ����۸�, �������, ��������)
                                log_and_send_im(f"{qmt_code}[{name}] ��ͣ�ⵥ������٣��������룬���µ�, ����۸�{fmt_float2str(����۸�)}, �������: {�������}")
                            else:
                                print(f"{qmt_code}[{name}] �������¹���������")
                continue
        else:
            if close <= ��ͣ�� and ��һ��� >= ��ʼ��ͣ�ⵥ���:  # ��ͣ���ҷⵥ����2000��
                has_code = False
                if len(g_final_df) > 0:
                    if len(g_final_df[g_final_df['qmt_code'] == qmt_code]) > 0:  # ��ɾ��
                        has_code = True
                if not has_code:
                    log_and_send_im(f"{qmt_code}[{name}] ��ͣ���ⵥ������ {��ʼ��ͣ�ⵥ���}�򣬽����ض���...")
                g_final_df.loc[qmt_code] = {'qmt_code': qmt_code, 'name': name, '��ʼ�����һ�ⵥ': ��һ���, '��ʼ��سɽ���': �ɽ���, '��ʼ��سɽ���': �ɽ���, '��ʼ���ʱ��': time.time(), '��ʼ���ʱ��_dt': get_curr_time()}
                print("############")
                print(g_final_df)
                return

            if len(g_final_df) > 0:
                curr_final_df = g_final_df[g_final_df['qmt_code'] == qmt_code].copy()
                if len(curr_final_df) > 0:
                    curr_final_data = curr_final_df.iloc[0]
                    ��ʼ���ʱ�� = curr_final_data['��ʼ���ʱ��']
                    ��ʼ��سɽ��� = curr_final_data['��ʼ��سɽ���']
                    ��ǰʱ�� = time.time()
                    print(f"--->>>>>{qmt_code}[{name}] cond1: {��ǰʱ�� - ��ʼ���ʱ�� <= �������}, �������: {�������},  cond2: {((�ɽ��� - ��ʼ��سɽ���) > ������������ٳɽ����)}, cond3: {(��һ��� <= ��������ⵥ���)}")
                    if (��ǰʱ�� - ��ʼ���ʱ�� <= �������) and ((�ɽ��� - ��ʼ��سɽ���) > ������������ٳɽ����) and (��һ��� <= ��������ⵥ���):
                        # ��������
                        if qmt_code not in g_�����¹��ĵ�_set:
                            g_�����¹��ĵ�_set.add(qmt_code)
                            ����۸� = pre_close * (100 - 9) / 100
                            ������� = int(�����ʽ� / ����۸� / 100) * 100
                            if g_�̶�����100��:
                                ������� = ������С����

                            qu.buy_stock(ContextInfo, qmt_code, name, ����۸�, �������, ��������)
                            log_and_send_im(f"{qmt_code}[{name}] ��ͣ�ⵥ������٣��������룬���µ�, ����۸�{fmt_float2str(����۸�)}, �������: {�������}")
                        else:
                            print(f"{qmt_code}[{name}] �������¹���������")


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
