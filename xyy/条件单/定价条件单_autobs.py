# encoding:gbk

"""
��������������

���ã���Ҫ�������֣�ִ��һ�δ�������������ʧЧ��

"""

import bsea_utils.bsea_xyy_qmt_util as qu
from bsea_utils.bsea_xyy_util import *

�������� = '����BS������'
table_t = 'bsea_����bs������'


def handlebar(ContextInfo):
    print(f'���� {��������} handlebar �е� 3��һ�ε�tick ~~~')

    curr_date = get_curr_date()
    curr_time = get_curr_time()
    curr_dtime = curr_date + " " + curr_time

    sql_all_��� = "SELECT * FROM " + table_t + " WHERE status='1' AND account_nick='" + str(cst.account_nick) + "'"
    all_df = get_df_from_table(sql_all_���)
    if len(all_df) == 0:
        print(f"{curr_dtime} {��������} {table_t} ��Ч���Ϊ�գ�����")
        return

    print(all_df)
    curr_date = get_curr_date()
    for index, row in all_df.iterrows():
        qmt_code = row['qmt_code']
        name = qu.get_name_by_qmtcode(ContextInfo, qmt_code)

        pk_id = get_num_by_numfield(row, 'id')
        ���������� = get_str_by_strfield(row, '����������')
        if ���������� != ����������.��������.value and ���������� != ����������.��������.value:
            log_and_send_im_with_ttl(f"{curr_dtime} {pk_id} ����������: {����������} ���ô������飡")
            continue

        df = ContextInfo.get_market_data(fields=['volume', 'close'], stock_code=[qmt_code], period='1d', dividend_type='front', count=1)
        if len(df) == 0 or df.iloc[0]['volume'] == 0:  # �ж�volume��Ϊ�˹���ͣ��
            log_and_send_im_with_ttl(f"{curr_dtime} {��������} {qmt_code}[{name}] ��ȡ��������ʧ�ܣ�����")
            continue
        curr_data = df.iloc[0]
        ��ǰ�۸� = curr_data['close']
        where_clause = " WHERE id='" + str(pk_id) + "'"

        if ���������� == ����������.��������.value:
            ����۸� = get_num_by_numfield(row, '���׼۸�')
            �������� = get_num_by_numfield(row, '���׹���')
            �����ֹ��Ч��db = get_dtime_by_datefield(row, '���׽�ֹ����')
            �����ֹ��Ч�� = �����ֹ��Ч��db if �����ֹ��Ч��db is not None else curr_date
            is_valid_�������� = False
            if ����۸� > 0 and �������� >= 100 and �����ֹ��Ч�� >= curr_date:
                is_valid_�������� = True

            if not is_valid_��������:
                log_and_send_im_with_ttl(f"{curr_dtime} {��������} {qmt_code}[{name}] {����۸�} {��������} {�����ֹ��Ч��}  {int(pk_id)}����������Ч�����飡����������")
                continue
            else:
                if ��ǰ�۸� <= ����۸�:
                    �˻������ʽ� = qu.get_�����ʽ�()
                    �ʽ����������� = int(�˻������ʽ� / ��ǰ�۸� / 100) * 100
                    ������� = min(��������, �ʽ�����������)
                    if ������� < 100:
                        log_and_send_im_with_ttl(f"{curr_dtime} {��������} {qmt_code}[{name}] �ﵽ�������������������Ϊ�㡣db���������{��������}, �ʽ�����������: {�ʽ�����������}, �˻������ʽ�: {�˻������ʽ�}")
                        continue
                    ������� = 100  # todo: ��ɾ��

                    qu.buy_stock_he(ContextInfo, qmt_code, name, �������, ��������)

                    update_sql = "UPDATE " + table_t + " SET status='0', lastmodified='" + get_lastmodified() + "'" + where_clause
                    save_or_update_by_sql(update_sql)


        elif ���������� == ����������.��������.value:
            �����۸� = get_num_by_numfield(row, '���׼۸�')
            �������� = get_num_by_numfield(row, '���׹���')
            ������ֹ��Ч��_db = get_dtime_by_datefield(row, '���׽�ֹ����')
            ������ֹ��Ч�� = ������ֹ��Ч��_db if ������ֹ��Ч��_db is not None else curr_date
            is_valid_�������� = False
            if �����۸� > 0 and �������� >= 100 and ������ֹ��Ч�� >= curr_date:
                is_valid_�������� = True

            if not is_valid_��������:
                log_and_send_im_with_ttl(f"{curr_dtime} {��������} {qmt_code}[{name}] {����۸�} {��������} {�����ֹ��Ч��} {int(pk_id)}����������Ч�����飡����������")
                continue
            else:
                if ��ǰ�۸� >= �����۸�:
                    ��ǰ�ֹ��� = qu.get_��������_by_qmtcode(qmt_code)
                    �������� = min(��������, ��ǰ�ֹ���)
                    if �������� < 100:
                        log_and_send_im_with_ttl(f"{curr_dtime} {��������} {qmt_code}[{name}] �ﵽ��������������������Ϊ�㡣db����������{��������}, �ֲֿ�������: {��ǰ�ֹ���}")
                        continue
                    �������� = 100  # todo����ɾ��

                    qu.sell_stock_he(ContextInfo, qmt_code, name, ��������, ��������)

                    update_sql = "UPDATE " + table_t + " SET status='0', lastmodified='" + get_lastmodified() + "'" + where_clause
                    save_or_update_by_sql(update_sql)


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
