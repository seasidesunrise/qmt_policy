# encoding:gbk

"""
qmt �µ�����

�������Ͼ��ۺ��ȡbuy_info���ݱ������µ����Ϳ��̺��ȡsell_info���������
�����¹ɡ���ծ�Զ��깺����ծ��ع���09:59�ֿ�ʼ��

��ν������������������߼���
1��������Ҫ�����ģ���������=1�������ۺ˰�ť����
2����������ͣ������ͣ��������
3��һ�ְ岻�ƣ��ֹɲ�����һ�ְ忪�壬�����˰�ť
4����������������Ͼ��۹ҵ�ͣ����

"""

import bsea_utils.bsea_xyy_qmt_util as qu
from bsea_utils.bsea_xyy_util import *

�������� = 'BSEA'

bsea_buy_table_t = 'bsea_buy_info'
bsea_sell_table_t = 'bsea_sell_info'

g_countdown_latch = 0
g_һ�ְ�_df = pd.DataFrame()


def timerHandler(ContextInfo):
    curr_time = get_curr_time()
    curr_date = get_curr_date()

    global g_һ�ְ�_df
    print(f'------$$$$$$ {��������} timerHandler��ʱ�� {curr_date} {curr_time}')

    if curr_time > '09:22:20' and curr_time < '09:25:00':  # �����յ�ͣ�������
        # ������Ҫ���������Ĺ�Ʊ
        sell_df2 = get_df_from_table("SELECT * FROM " + bsea_sell_table_t + " WHERE sell_dtime='" + get_curr_date() + "' AND ��������=1 AND status=1 ORDER BY ���� ASC, �Ӳ��� ASC")
        print(sell_df2)
        if len(sell_df2) == 0:
            print(f"{��������} �� �������̵�ͣ�����տ������� �ı��")
        else:
            �����ʽ�, �ֲ�df, obj_list = qu.get_stock_�ֲ��б�()
            �����ֲ�df = �ֲ�df[�ֲ�df['��������'] > 0].copy()
            for index33, row33 in sell_df2.iterrows():
                qmt_code = row33['qmt_code']
                ���� = row33['����']
                name = qu.get_name_by_qmtcode(qmt_code)
                tmpdf = �����ֲ�df[�����ֲ�df['qmt_code'] == qmt_code].copy()
                if len(tmpdf):
                    tmpdata = tmpdf.iloc[0]
                    �������� = tmpdata['��������']
                    qu.sell_stock_he(ContextInfo, qmt_code, name, ��������, ��������)  # �˰�ť����
                    save_or_update_by_sql("UPDATE " + bsea_sell_table_t + " SET status=0 WHERE qmt_code='" + qmt_code + "' AND dtime='" + str(curr_date) + "' AND ����='" + ���� + "'")

    if curr_time > '09:25:20' and curr_time < '09:30:00':  # ����
        # �����ñ��Ƿ�׼������������
        is_prepared = is_���ۿ���_prepared()
        if is_prepared:
            # ��ȡ�����
            df = get_df_from_table("SELECT * FROM " + bsea_buy_table_t + " WHERE dtime='" + curr_date + "' AND status=1 ORDER BY ���� ASC, �Ƽ����� ASC")
            print(df)
            if len(df) == 0:
                print(�������� + " " + curr_date + " ������xg���������")
            else:
                for index, row in df.iterrows():
                    name = row['name']
                    qmt_code = row['qmt_code']  # ��'600000.SH'
                    ���� = row['����']
                    ����۸� = row['����۸�']
                    ������� = row['�������']

                    qu.buy_stock(ContextInfo, qmt_code, name, ����۸�, �������, ����)
                    save_or_update_by_sql("UPDATE " + bsea_buy_table_t + " SET status=0 WHERE qmt_code='" + qmt_code + "' AND dtime='" + str(curr_date) + "' AND ����='" + ���� + "'")

        # ���һ�ְ忪�̣�д��ȫ�ֱ���
        if len(g_һ�ְ�_df) == 0:
            sell_df = get_sell_infos()
            for index, row in sell_df.iterrows():
                code = row['code']
                qmt_code = row['qmt_code']
                name = row['name']
                pre_close = row['pre_close']
                һ�ְ�, ��ͣ�� = qu.is_����һ�ְ�_by_qmt(ContextInfo, qmt_code, pre_close)
                if һ�ְ�:
                    print(�������� + " " + code + " ���̶�һ�ְ壨��һֱδ����ͳ��У���������̣�")
                    g_һ�ְ�_df = g_һ�ְ�_df.append({'code': code, 'name': name, '��ͣ��': ��ͣ��}, ignore_index=True)  #

    if (curr_time >= '09:30:00' and curr_time < '11:33:00') or (curr_time >= '12:57:00' and curr_time < '15:03:00'):  # ����
        # ��ǰ�ֲֲ�ѯ
        �����ʽ�, �ֲ�df, obj_list = qu.get_stock_�ֲ��б�()
        �����ֲ�df = �ֲ�df[�ֲ�df['��������'] > 0].copy()

        # ��ѯ������
        sell_df = get_sell_infos()
        print(sell_df)

        for index, row in sell_df.iterrows():
            code = row['code']
            qmt_code = row['qmt_code']
            ���� = row['����']
            name = row['name']
            pre_close = row['pre_close']

            # ���㵱����ͣ����ͣ��
            ������ͣ��, ���յ�ͣ�� = qu.get_��ͣ_��ͣ��_by_qmt(ContextInfo, qmt_code)
            print(f"{��������} {code}[{name}], pre_close: {pre_close}, ��ͣ��: {������ͣ��}, ��ͣ��: {���յ�ͣ��}")
            tmpdf = �����ֲ�df[�����ֲ�df['code'] == code].copy()
            if len(tmpdf) > 0:
                tmpdata = tmpdf.iloc[0]
                ��ǰ�� = tmpdata['��ǰ��']
                �������� = tmpdata['��������']
                �����Ƿ� = 100 * (��ǰ�� - pre_close) / pre_close
                print(f"{��������} {code}[{name}] ��ǰ��: {fmt_float2str(��ǰ��)}, ��ǰ�Ƿ�: {fmt_float2str(�����Ƿ�)}")

                if �������� > 0:
                    # �鿴�Ƿ�һ�ְ�����
                    is_����һ�ְ� = False
                    if len(g_һ�ְ�_df) > 0:
                        һ�ְ�tmpdf = g_һ�ְ�_df[g_һ�ְ�_df['code'] == code]
                        if len(һ�ְ�tmpdf) > 0:  # ����һ�ְ��case
                            is_����һ�ְ� = True
                            һ�ְ�tmpdata = һ�ְ�tmpdf.iloc[0]
                            ��ͣ�� = һ�ְ�tmpdata['��ͣ��']
                            if ��ǰ�� < ��ͣ��:  # �ư壬�����ҵ�ͣ������
                                qu.sell_stock(ContextInfo, qmt_code, name, ���յ�ͣ��, ��������, ����)

                    if not is_����һ�ְ�:
                        if curr_time > '14:56:00':
                            # �ȳ���
                            qu.cancel_all_order(ContextInfo, cst.account, ����)
                            if curr_time > '14:58:00':
                                qu.sell_stock(ContextInfo, qmt_code, name, ���յ�ͣ��, ��������, ����)  # �Ҿ��۵�, ֱ�ӹҵ�ͣ���������ջ������̼۳ɽ�
                        else:
                            if �������� > 0:
                                if ��ǰ�� >= ������ͣ�� - 0.01:  # ����ͣ�����
                                    qu.sell_stock(ContextInfo, qmt_code, name, (������ͣ�� - 0.01), ��������, ����)  # ����ͣ������
                                elif ��ǰ�� <= ���յ�ͣ�� + 0.01:  # ����ͣ�����
                                    qu.sell_stock(ContextInfo, qmt_code, name, ���յ�ͣ��, ��������, ����)  # ����ͣ������

    if (curr_time >= '09:59:40' and curr_time < '10:02:00'):  # �¹�_��ծ_�깺
        qu.�¹�_��ծ_�깺(ContextInfo)

    if (curr_time >= '11:33:00' and curr_time < '12:57:00'):
        print(f"{��������} ����������, sleep 15s")
        time.sleep(15)

    if (curr_time >= '15:03:00' and curr_time < '15:33:00'):  # ȫ���ʽ�all in��ծ��ع�1����, �з��ظ��µ����ܣ�ʵ��Ǯ����Ҳ�������ظ����롣��ع�����ʱ���ӳ���15:30
        qu.��ծ��ع�(ContextInfo, cst.account)
        print(f"{��������} ������, sleep 30s")
        time.sleep(30)

    if curr_time >= '15:33:00':  # ������
        print(f"{��������} ������, sleep 300s")
        time.sleep(600)


def init(ContextInfo):
    log_and_send_im(f"------$$$$$$ {get_curr_date()} {get_curr_time()} {��������} ������init")
    pass_qmt_funcs()

    ContextInfo.set_account(cst.account)
    timer_startTime = get_curr_date() + "09:20:10"

    ContextInfo.run_time("timerHandler", "3nSecond", timer_startTime)


def handlebar(ContextInfo):
    print(f'{��������} ���� handlebar �е� say hi~~~')

    d = ContextInfo.barpos
    realtime = ContextInfo.get_bar_timetag(d)
    nowdate = timetag_to_datetime(realtime, '%Y-%m-%d %H:%M:%S')
    print(nowdate)

    global g_countdown_latch
    g_countdown_latch -= 1
    if g_countdown_latch <= 0:
        g_countdown_latch = 8
        �����ʽ�, �ֲ�df, obj_list = qu.get_stock_�ֲ��б�()


def get_sell_infos():
    """ ��ѯ������ """
    sql2 = "SELECT * FROM " + bsea_sell_table_t + " WHERE sell_dtime='" + get_curr_date() + "' AND status=1 ORDER BY ���� ASC, �Ӳ��� ASC"
    sell_df = get_df_from_table(sql2)
    return sell_df


def is_���ۿ���_prepared():
    """ �����ñ��Ƿ�׼������������ """
    is_prepared = False

    conf_sql = "SELECT * FROM xyy_config WHERE conf='�����Զ��µ�'"
    conf_df = get_df_from_table(conf_sql)
    if len(conf_df) == 0:
        log_and_send_im(f"{��������} �޾����Զ��µ����ã�����xyy_config���Ӧ��������")
        is_prepared = False
    else:
        conf_data = conf_df.iloc[0]
        if conf_data['val'] != '1':
            log_and_send_im(f"{��������} ������������δ׼���ã���ȴ�xg����'�����Զ��µ�'��Ӧ��valֵ����Ϊ1...")
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
    qu.stop_policy(��������)


def deal_callback(ContextInfo, dealInfo):
    """ ���˺ųɽ�״̬�б仯ʱ����ִ��������� """
    qu.deal_callback_func(dealInfo, ��������)
