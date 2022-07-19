# encoding:gbk

"""
������T2����: ����ĳ������5%����������������������۵�97%���

���߼���
1������30��5��������;
2��������������۵�97%��أ�
3������60����ֹ��ȫ������;
4�����ݸ���period��'5m', '15m', '30m', '1h', '1d', '1w', '1mon'
5�����롢������������2%�����µ���û�ɽ���û�ɽ���

todo��
1��ֹ��ʱ���Ƿ�Ҫ����������

"""

import bsea_utils.bsea_xyy_qmt_util as qu
from bsea_utils.bsea_xyy_util import *

�������� = '������T2'
table_t = "bsea_��t_����2_period"


def handlebar(ContextInfo):
    print(f'{��������} ���� handlebar �е� 3��һ�ε�tick ~~~')

    sql_all_��� = "SELECT * FROM " + table_t + " WHERE status='1' AND account_nick='" + str(cst.account_nick) + "'"
    all_df = get_df_from_table(sql_all_���)
    if len(all_df) == 0:
        print(f"{��������} {table_t} ��Ч���Ϊ�գ�����")
        return

    for index, row in all_df.iterrows():
        qmt_code = row['qmt_code']
        name = qu.get_name_by_qmtcode(ContextInfo, qmt_code)

        ��t���� = get_num_by_numfield(row, '��t����')
        ��tֹ����� = get_num_by_numfield(row, '��tֹ�����')
        ���ھ��߰ٷֱ����� = get_num_by_numfield(row, '���ھ��߰ٷֱ�����')  # ��5������ʾ���ھ���5%����
        ��t�ʽ� = get_num_by_numfield(row, '��t�ʽ�')  # ��ǰ��t֧����ʽ���
        rt_��ǰ��t״̬ = get_str_by_strfield(row, 'rt_��ǰ��t״̬')
        period = get_str_by_strfield(row, 'period')  # ����
        if period is None or period not in qu.period_list:
            log_and_send_im(f"{��������} {qmt_code}[{name}] period ���ô��󣬱���Ϊ��{qu.period_list} ����֮һ�����飬������T���Ժ��ԣ���")
            continue
        if ��t���� <= 1:
            log_and_send_im(f"{��������} {qmt_code}[{name}] �������ô��� ��t���ߣ�{��t����}�����飬������T���Ժ��ԣ���")
            continue
        if ��tֹ����� <= 1:
            log_and_send_im(f"{��������} {qmt_code}[{name}] �������ô��� ֹ����ߣ�{��tֹ�����}�����飬������T���Ժ��ԣ���")
            continue

        df = qu.get_quatation_by_params(ContextInfo, qmt_code, period, ��t����, ��tֹ�����)
        curr_data = df.iloc[-1]
        ��ǰ�۸� = curr_data['close']
        where_clause = " WHERE qmt_code='" + qmt_code + "' AND account_nick='" + cst.account_nick + "'"

        if ��tֹ����� < 1000 and curr_data['pre_close'] < curr_data['ma' + str(��tֹ�����)]:  # ֹ��
            # todo:  �������δ�ɽ��ĵ��ӣ��Ƿ�Ҫ��57��֮ǰ�ȳ���
            �������� = qu.get_��������_by_qmtcode(qmt_code)
            if �������� == 0:
                log_and_send_im_with_ttl(f"{��������} {table_t} {qmt_code}[{name}] �ﵽֹ����������������������Ϊ 0�����˹���飡��")
                continue

            �������� = 100  # todo����λ�������ڼ��ݶ�100��
            qu.sell_stock_he_2p(ContextInfo, qmt_code, name, ��ǰ�۸�, ��������, ��������)

            save_or_update_by_sql("UPDATE " + table_t + " SET status='0' " + where_clause)
            log_and_send_im(f"{��������} {qmt_code}[{name}] �ﵽֹ���������������µ���֣���")
        else:
            ��Ⱦ����Ƿ� = curr_data['��Ⱦ����Ƿ�']
            if (��Ⱦ����Ƿ� >= ���ھ��߰ٷֱ����� > 0) and (rt_��ǰ��t״̬ == '' or rt_��ǰ��t״̬ == '�����'):  # ��T����������
                �ֲֿ������� = qu.get_��������_by_qmtcode(qmt_code)
                ��t�������� = int(��t�ʽ� / ��ǰ�۸� / 100) * 100
                �������� = min(��t��������, �ֲֿ�������)  # ȡdb�еĵ�ǰ�ֹ�����ֲ��еĿ���������ȡ����С���Ǹ������� todo����ǰ�ֹ����߼���Ҫ�����޸ģ������ڼ��Ⱥ���
                if �������� == 0:
                    log_and_send_im_with_ttl(f"{��������} {qmt_code}[{name}] �ﵽ��������������������Ϊ�㡣��t����������{��t��������}, �ֲֿ�������: {�ֲֿ�������}")
                    continue
                �������� = 100  # todo: ��λ�������ڼ��ݶ�100��

                qu.sell_stock_he_2p(ContextInfo, qmt_code, name, ��ǰ�۸�, ��������, ��������)

                ��������ؼ��������۵İٷֱ� = get_num_by_numfield(row, '��������ؼ��������۵İٷֱ�')
                rt_��ؼ۸� = ��ǰ�۸� * ��������ؼ��������۵İٷֱ� / 100
                t_status = T_Type.��t��.value
                update_sql = "UPDATE " + table_t + " SET rt_��ǰ��t״̬='" + t_status + "', rt_��ؼ۸�='" + str(rt_��ؼ۸�) + "', rt_��ǰ�ֹ���='" + str(0) + "' " + where_clause
                save_or_update_by_sql(update_sql)
                continue

            if (rt_��ǰ��t״̬ == '' or rt_��ǰ��t״̬ == '��t��'):  # ��T�����������µ����
                t��ȫ���ɽ� = qu.check_ί���Ƿ���ȫ���ɽ�(qmt_code)
                if not t��ȫ���ɽ�:
                    log_and_send_im_with_ttl(f"{��������} {qmt_code}[{name}] t��ȫ���ɽ�: {t��ȫ���ɽ�}, �ȴ������ĵ��ӳɽ�", 30)
                    continue
                rt_��ؼ۸� = get_num_by_numfield(row, 'rt_��ؼ۸�')
                if ��ǰ�۸� <= rt_��ؼ۸�:  # �۸������ؼ۸��µ����
                    �˻������ʽ� = qu.get_�����ʽ�()
                    �˻��ʽ����������� = int(�˻������ʽ� / ��ǰ�۸� / 100) * 100
                    ��t�ʽ�������� = int(��t�ʽ� / ��ǰ�۸� / 100) * 100
                    ������� = min(�˻��ʽ�����������, ��t�ʽ��������)
                    if ������� < 100:
                        log_and_send_im_with_ttl(f"{��������} {qmt_code}[{name}] �ﵽ�������������������������һ�֡��˻��ʽ�������������{�˻��ʽ�����������}, ��t�ʽ����������{��t�ʽ��������}, ��t�ʽ�: {��t�ʽ�}")
                        continue
                    ������� = 100  # todo: ��λ�������ڼ��ݶ�100��

                    qu.buy_stock_he_2p(ContextInfo, qmt_code, name, ��ǰ�۸�, �������, ��������)  # �˰�ť��

                    t_status = T_Type.�����.value
                    update_sql = "UPDATE " + table_t + " SET rt_��ǰ��t״̬='" + t_status + "', rt_��ǰ�ֹ���='" + str(�������) + "' " + where_clause
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


def deal_callback(ContextInfo, dealInfo):
    """ ���˺ųɽ�״̬�б仯ʱ����ִ��������� """
    qu.deal_callback_func(dealInfo, ��������)


def stop(ContextInfo):
    qu.stop_policy(��������)
