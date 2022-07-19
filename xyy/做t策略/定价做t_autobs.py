# encoding:gbk

"""
������T����

���ã����ڽ�����ɺ���׵���

���߼���
1������ĳ�۸�price_high����;
2������ĳ�۸�price_low����;
3������ĳ�۸�price_zsֹ������;
4�����롢�����������ú˰�ť�µ������ʽ�����С��Ʊ����ʹ�ã�

"""

import bsea_utils.bsea_xyy_qmt_util as qu
from bsea_utils.bsea_xyy_util import *

�������� = '����bs��t'
table_t = 'bsea_��t_����'


def handlebar(ContextInfo):
    print(f'���� {��������} handlebar �е� 3��һ�ε�tick ~~~')

    curr_date = get_curr_date()
    curr_time = get_curr_time()
    curr_dtime = curr_date + " " + curr_time

    sql_all_��� = "SELECT * FROM " + table_t + " WHERE status='1' AND account_nick='" + str(cst.account_nick) + "'"
    all_df = get_df_from_table(sql_all_���)
    if len(all_df) == 0:
        print(f"{curr_dtime} {��������} ��Ч���Ϊ�գ�����")
        return

    for index, row in all_df.iterrows():
        qmt_code = row['qmt_code']
        name = qu.get_name_by_qmtcode(ContextInfo, qmt_code)

        price_high = get_num_by_numfield(row, 'price_high')
        price_low = get_num_by_numfield(row, 'price_low')
        price_zs = get_num_by_numfield(row, 'price_zs')
        ��t�ʽ� = get_num_by_numfield(row, '��t�ʽ�')  # ��ǰ��t֧����ʽ�������T�ʽ�
        rt_��ǰ�ֹ��� = get_num_by_numfield(row, 'rt_��ǰ�ֹ���')
        rt_��ǰ��t״̬ = get_str_by_strfield(row, 'rt_��ǰ��t״̬')

        df = ContextInfo.get_market_data(fields=['volume', 'close'], stock_code=[qmt_code], period='1d', dividend_type='front', count=1)
        if len(df) == 0 or df.iloc[0]['volume'] == 0:  # �ж�volume��Ϊ�˹���ͣ��
            log_and_send_im_with_ttl(f"{curr_dtime} {��������} {qmt_code}[{name}] ��ȡ��������ʧ�ܣ�����")
            continue
        curr_data = df.iloc[0]
        ��ǰ�۸� = curr_data['close']

        if ��ǰ�۸� < price_zs:  # ����ֹ����ߣ�ֹ��
            �������� = qu.get_��������_by_qmtcode(cst.account, qmt_code)
            if �������� == 0:
                log_and_send_im_with_ttl(f"{curr_dtime} {��������} {qmt_code}[{name}] �ﵽֹ����������������������Ϊ 0")
                continue

            �������� = 100  # todo��Ӧ��ȫ������
            qu.sell_stock_he(ContextInfo, qmt_code, name, ��������, ��������)  # ���۸����ӵĺ˰�ť����

            save_or_update_by_sql("UPDATE " + table_t + " SET status='0', lastmodified='" + get_lastmodified() + "' WHERE qmt_code='" + qmt_code + "'")
            log_and_send_im(f"{curr_dtime} {��������} {qmt_code}[{name}] ��������: {��������} �ﵽֹ����������������֣���")
        else:
            where_clause = " WHERE qmt_code='" + qmt_code + "' AND account_nick='" + cst.account_nick + "'"
            if (rt_��ǰ��t״̬ == '' or rt_��ǰ��t״̬ == '��T��') and (price_zs < ��ǰ�۸� <= price_low):  # �տ�ʼ�����Ѿ����� ���۸���������λ�ã�ִ��'���'����
                ������� = int(��t�ʽ� / ��ǰ�۸� / 100) * 100
                if ������� < 100:
                    log_and_send_im_with_ttl(f"{curr_dtime} {��������} {qmt_code}[{name}] ������� ����100�ɣ���t�ʽ�{��t�ʽ�}")
                    continue
                ������� = 100  # todo: ��λ��С��Ҫ

                qu.buy_stock_he(ContextInfo, qmt_code, name, �������, ��������)

                t_status = T_Type.�����.value
                update_sql = "UPDATE " + table_t + " SET rt_��ǰ��t״̬='" + t_status + "', rt_��ǰ�ֹ���='" + str(�������) + "', lastmodified='" + get_lastmodified() + "'" + where_clause
                save_or_update_by_sql(update_sql)
                continue

            if (rt_��ǰ��t״̬ == '' or rt_��ǰ��t״̬ == '�����') and (��ǰ�۸� >= price_high):  # �տ�ʼ�����Ѿ��� ���۸�������������λ�ã�ִ��'����'����
                �������� = qu.get_��������_by_qmtcode(cst.account, qmt_code)
                ��t�ʽ��������� = int(��t�ʽ� / ��ǰ�۸� / 100) * 100
                �������� = min(rt_��ǰ�ֹ���, ��t�ʽ���������, ��������)  # ȡdb�еĵ�ǰ�ֹ�����ֲ��еĿ���������ȡ����С���Ǹ�����
                if �������� < 100:
                    log_and_send_im_with_ttl(f"{curr_dtime} {��������} {qmt_code}[{name}] �ﵽ��������������������Ϊ�㡣db����������{rt_��ǰ�ֹ���}, �ֲֿ�������: {��������}")
                    continue
                �������� = 100  # todo: ��λ��С��Ҫ

                qu.sell_stock_he(ContextInfo, qmt_code, name, ��������, ��������)

                t_status = T_Type.��t��.value
                update_sql = "UPDATE " + table_t + " SET rt_��ǰ��t״̬='" + t_status + "', rt_��ǰ�ֹ���='" + str(0) + "', lastmodified='" + get_lastmodified() + "'" + where_clause
                save_or_update_by_sql(update_sql)
                continue


def init(ContextInfo):
    log_and_send_im(f"------$$$$$$ {get_curr_date()}  {get_curr_time()}  {��������} ����������init")
    pass_qmt_funcs()
    ContextInfo.set_account(cst.account)


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
