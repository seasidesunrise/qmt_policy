# encoding:gbk

"""
������T����: ����ĳ������5%���������ھ���5%����

���߼���
1������30��5��������;
2������30��5��������;
3������60����ֹ��ȫ������;
4�����ݸ���period��'5m', '15m', '30m', '1h', '1d', '1w', '1mon'
5�����롢�����������ú˰�ť�µ������ʽ�����С��Ʊ����ʹ�ã�

todo��
1��ֹ��ʱ���Ƿ�Ҫ����������

"""
import talib

import bsea_utils.bsea_xyy_qmt_util as qu
from bsea_utils.bsea_xyy_util import *

�������� = '������T'  # ��������
table_t = "bsea_��t_����_period"


def handlebar(ContextInfo):
    print('���� handlebar �е� 3��һ�ε�tick ~~~')

    sql_all_��� = "SELECT * FROM " + table_t + " WHERE status='1' AND account_nick='" + str(cst.account_nick) + "'"
    all_df = get_df_from_table(sql_all_���)
    if len(all_df) == 0:
        print(f"{��������} {table_t} ��Ч���Ϊ�գ�����")

    for index, row in all_df.iterrows():
        qmt_code = row['qmt_code']
        name = qu.get_name_by_qmtcode(ContextInfo, qmt_code)

        ��t���� = get_num_by_numfield(row, '��t����')
        ��tֹ����� = get_num_by_numfield(row, '��tֹ�����')
        ���ھ��߰ٷֱ����� = get_num_by_numfield(row, '���ھ��߰ٷֱ�����')  # ��5������ʾ���ھ���5%����
        ���ھ��߰ٷֱ����� = get_num_by_numfield(row, '���ھ��߰ٷֱ�����')  # ��5������ʾ���ھ���5%����
        ��t�ʽ� = get_num_by_numfield(row, '��t�ʽ�')  # ��ǰ��t֧����ʽ���
        rt_��ǰ��t״̬ = get_str_by_strfield(row, 'rt_��ǰ��t״̬')
        rt_��ǰ��t�ʽ� = get_num_by_numfield(row, 'rt_��ǰ��t�ʽ�')
        rt_��ǰ�ֹ��� = get_str_by_strfield(row, 'rt_��ǰ�ֹ���')
        period = get_str_by_strfield(row, 'period')  # ����

        if period is None or period not in qu.period_list:
            log_and_send_im(f"{��������} {table_t} {qmt_code} {name} period ���ô��󣬱���Ϊ��{qu.period_list} ����֮һ�����飬������T���Ժ��ԣ���")
            continue
        if ��t���� <= 1:
            log_and_send_im(f"{��������} {table_t} {qmt_code} {name} �������ô��� ��t���ߣ�{��t����}�����飬������T���Ժ��ԣ���")
            continue
        if ��tֹ����� <= 1:
            log_and_send_im(f"{��������} {table_t} {qmt_code} {name} �������ô��� ֹ����ߣ�{��tֹ�����}�����飬������T���Ժ��ԣ���")
            continue

        ma_colname = 'ma' + str(��t����)
        df = get_quatation_by_params(ContextInfo, qmt_code, period, ��t����, ��tֹ�����)
        curr_data = df.iloc[-1]

        ��ǰ�۸� = curr_data['close']
        ֹ��۸� = curr_data['ma' + str(��tֹ�����)]
        where_clause = " WHERE qmt_code='" + qmt_code + "' AND account_nick='" + cst.account_nick + "'"

        if curr_data['pre_close'] < curr_data['ma' + str(��tֹ�����)]:  # ֹ��
            �������� = qu.get_��������_by_qmtcode(cst.account, qmt_code)
            if �������� == 0:
                log_and_send_im(f"{��������} {qmt_code} {name} �ﵽֹ����������������������Ϊ 0")
                continue

            �������� = 100  # todo��Ӧ��ȫ������
            qu.he_sell_stock(ContextInfo, qmt_code, name, ��������, ��������)  # �˰�ť��

            save_or_update_by_sql("UPDATE " + table_t + " SET status='0' " + where_clause)
            log_and_send_im(f"{��������} {qmt_code} {name} �ﵽֹ���������������µ���֣���")
        else:
            ��Ⱦ����Ƿ� = curr_data['��Ⱦ����Ƿ�']
            # ���ƫ����߷���
            if (��Ⱦ����Ƿ� >= ���ھ��߰ٷֱ�����) and (rt_��ǰ��t״̬ == '' or rt_��ǰ��t״̬ == '�����'):  # ��T����������
                rt_��ǰ��t�ʽ� = rt_��ǰ��t�ʽ� if rt_��ǰ��t�ʽ� != 0 else ��t�ʽ�
                �ֲֿ������� = qu.get_��������_by_qmtcode(qmt_code)
                ��t�������� = int(rt_��ǰ��t�ʽ� / ��ǰ�۸� / 100) * 100
                �������� = min(��t��������, �ֲֿ�������)  # ȡdb�еĵ�ǰ�ֹ�����ֲ��еĿ���������ȡ����С���Ǹ������� todo����ǰ�ֹ����߼���Ҫ�����޸ģ������ڼ��Ⱥ���
                if �������� == 0:
                    print(f"{��������} {qmt_code} {name} �ﵽ��������������������Ϊ�㡣��t����������{��t��������}, �ֲֿ�������: {�ֲֿ�������}")
                    continue
                �������� = 100  # todo: ��λ�������ڼ��ݶ�100��

                qu.he_sell_stock(ContextInfo, qmt_code, name, ��������, ��������)  # �˰�ť��

                rt_��ǰ��t�ʽ� += �������� * ��ǰ�۸�
                t_status = T_Type.��t��.name
                update_sql = "UPDATE " + table_t + " SET ��ǰ��t״̬='" + t_status + "', ��t�ʽ�='" + str(��t�ʽ�) + "', ��ǰ�ֹ���='" + str(0) + "' " + where_clause
                save_or_update_by_sql(update_sql)

        t��ȫ���ɽ� = qu.check_ί���Ƿ���ȫ���ɽ�(qmt_code)
        if t��ȫ���ɽ� and (��Ⱦ����Ƿ� <= -���ھ��߰ٷֱ����� < 0) and (rt_��ǰ��t״̬ == '' or rt_��ǰ��t״̬ == '��T��'):  # ��T���������
            rt_��ǰ��t�ʽ� = rt_��ǰ��t�ʽ� if rt_��ǰ��t�ʽ� != 0 else ��t�ʽ�
            ������� = int(��t�ʽ� / ��ǰ�۸� / 100) * 100
            ������� = 100  # todo: ��λ��С��Ҫ

            qu.he_buy_stock(ContextInfo, qmt_code, name, �������, ��������)  # �˰�ť��

            rt_��ǰ��t�ʽ� -= ������� * ��ǰ�۸�
            t_status = T_Type.�����.name
            update_sql = "UPDATE " + table_t + " SET ��ǰ��t״̬='" + t_status + "', rt_��ǰ��t�ʽ�='" + str(rt_��ǰ��t�ʽ�) + "', ��ǰ�ֹ���='" + str(�������) + "' " + where_clause
            save_or_update_by_sql(update_sql)

    d = ContextInfo.barpos
    realtime = ContextInfo.get_bar_timetag(d)
    nowdate = timetag_to_datetime(realtime, '%Y-%m-%d %H:%M:%S')
    print(nowdate)


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


def get_quatation_by_params(ContextInfo, qmt_code, period, ��t����, ֹ�����):
    df = ContextInfo.get_market_data(fields=['volume', 'amount', 'open', 'high', 'low', 'close'], stock_code=[qmt_code], period=period, dividend_type='front', count=max(��t����, ֹ�����) + 10)
    ma_colname = 'ma' + str(��t����)
    df[ma_colname] = talib.MA(df['close'], ��t����)
    df['ma' + str(ֹ�����)] = talib.MA(df['close'], ֹ�����)
    df['pre_close'] = df['close'].shift(1)
    df['�Ƿ�'] = 100 * (df['close'] - df['pre_close']) / df['pre_close']
    df['��Ⱦ����Ƿ�'] = 100 * (df['close'] - df[ma_colname]) / df[ma_colname]
    return df


def deal_callback(ContextInfo, dealInfo):
    """ ���˺ųɽ�״̬�б仯ʱ����ִ��������� """
    qu.deal_callback_func(dealInfo)


def stop(ContextInfo):
    log_and_send_im(f"------$$$$$$ {get_curr_date()}  {get_curr_time()}  {��������} ������ֹͣ��")
