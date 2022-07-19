# encoding:gbk

"""
���̹���T_�Ӷ� ����

"""

import bsea_utils.bsea_xyy_qmt_util as qu
from bsea_utils.bsea_xyy_util import *

�������� = '���̹���T_�Ӷ�'
table_t = 'bsea_���̹���t_�Ӷ�'

############  �˹�ָ�����ֿ�ʼ ###############

# ���ݿ��ֶν��ܣ����������� https://z7jmmgj5px.feishu.cn/docx/doxcnDKF1HszxL75RxQ7IpGF1nf

############  �˹�ָ�����ֽ��� ###############

g_countdown_latch = 8
g_data = {}


def timerHandler(ContextInfo):
    curr_time = get_curr_time()

    global g_data

    if not check_is_����_or_�ٽ�(curr_time):
        print(f"{��������} ��ǰʱ�䲻�ڽ����У� {curr_time}")
        return

    sql_all_��� = "SELECT * FROM " + table_t + " WHERE status='1' AND account_nick='" + str(cst.account_nick) + "'"
    all_df = get_df_from_table(sql_all_���)
    if len(all_df) == 0:
        print(f"{��������} {table_t} ��Ч���Ϊ�գ�����")

    for index, row in all_df.iterrows():
        qmt_code = row['qmt_code']
        name = qu.get_name_by_qmtcode(ContextInfo, qmt_code)
        �۲���ʼ�� = get_dtime_by_datefield(row, '�۲���ʼ��dtime')
        print(f"{��������} �۲���ʼ��: {�۲���ʼ��}")
        �Ƿ���t = (get_num_by_numfield(row, '�Ƿ���t') == 1)
        �ɽ�������������ֵ = get_num_by_numfield(row, '�ɽ�������������ֵ')
        rt_�ɽ�������dtime = get_dtime_by_datefield(row, 'rt_�ɽ�������dtime')
        period = get_str_by_strfield(row, 'period')  # ����
        if period is None or period not in qu.period_list:
            log_and_send_im(f"{��������} {qmt_code}[{name}] period ���ô��󣬱���Ϊ��{qu.period_list} ����֮һ�����飬������T���Ժ��ԣ���")
            continue

        df = ContextInfo.get_market_data(fields=['volume', 'amount', 'open', 'high', 'low', 'close'], stock_code=[qmt_code], period=period, dividend_type='front', start_time=�۲���ʼ��.replace('-', ''))
        df['pre_close'] = df['close'].shift(1)
        df['high�Ƿ�'] = 100 * (df['high'] - df['pre_close']) / df['pre_close']
        print(df)
        where_clause = " WHERE qmt_code='" + qmt_code + "' AND account_nick='" + cst.account_nick + "'"

        ii = 0
        for index2, row2 in df.iterrows():
            ii += 1
            volume = row2['volume'] / 10000  # תΪ�����
            dt = str(index2)
            dt2 = dt[:4] + "-" + dt[4:6] + "-" + dt[6:8] + dt[8:]

            if volume > �ɽ�������������ֵ:
                rt_�ɽ�������dt = dt2

                key = qmt_code + "_�ɽ�������"
                if g_data.get(key) is None:
                    g_data.update({key: '1'})
                    rt_�ɽ�������dtime = rt_�ɽ�������dt
                    log_and_send_im(f"{��������} {qmt_code}[{name}] �ɽ�������dtime dt:{dt}, rt_�ɽ�������dtime: {rt_�ɽ�������dtime}")
                    save_or_update_by_sql("UPDATE " + table_t + " SET rt_�ɽ�������dtime='" + rt_�ɽ�������dtime + "', �Ƿ���t='0' " + where_clause)

            if ii < len(df):  # ���һ������Ϊ���У�����k��δ����ȷ�ϣ������������Ӱ��
                ��Ӱ�߳�����ʵ�屶�� = get_num_by_numfield(row, '��Ӱ�߳�����ʵ�屶��')
                ��Ӱ����߼��Ƿ� = get_num_by_numfield(row, '��Ӱ����߼��Ƿ�')

                ��Ӱ�� = (row2['high'] - max(row2['open'], row2['close']))
                ʵ�� = abs(row2['open'] - row2['close'])
                ʵ�� = ʵ�� if ʵ�� > 0 else 0.0001
                if ��Ӱ�� / ʵ�� > ��Ӱ�߳�����ʵ�屶�� > 0 and row2['high�Ƿ�'] > ��Ӱ����߼��Ƿ� > 0 and dt2 >= rt_�ɽ�������dtime:  # ��Ӱ�߱���
                    rt_��Ӱ��dtime = dt[:4] + "-" + dt[4:6] + "-" + dt[6:8] + dt[8:]

                    key = qmt_code + "_" + rt_��Ӱ��dtime + "_��Ӱ��"
                    if g_data.get(key) is None:
                        g_data.update({key: '1'})
                        ��Ӱ�߳��ֺ�����������Ӱ����߼۰ٷֱ� = get_num_by_numfield(row, '��Ӱ�߳��ֺ�����������Ӱ����߼۰ٷֱ�')
                        rt_��Ӱ�ߺ������۸� = row2['high'] * ��Ӱ�߳��ֺ�����������Ӱ����߼۰ٷֱ� / 100
                        log_and_send_im(f"{��������} {qmt_code}[{name}] ��Ӱ��dtime dt:{dt}, rt_��Ӱ��dtime: {rt_��Ӱ��dtime}")
                        save_or_update_by_sql("UPDATE " + table_t + " SET rt_��Ӱ��dtime='" + rt_��Ӱ��dtime + "', rt_��Ӱ�ߺ������۸�='" + str(rt_��Ӱ�ߺ������۸�) + "' " + where_clause)


def init(ContextInfo):
    log_and_send_im(f"------$$$$$$ {get_curr_date()}  {get_curr_time()}  {��������} ����������init")

    pass_qmt_funcs()
    ContextInfo.set_account(cst.account)

    timer_startTime = get_curr_date() + "09:25:10"
    ContextInfo.run_time("timerHandler", "30nSecond", timer_startTime)


def handlebar(ContextInfo):
    print(f'{��������} ���� handlebar �е� 3��һ�ε�tick ~~~')
    curr_time = get_curr_time()

    if not check_is_����_or_�ٽ�(curr_time):
        return

    global g_data

    sql_all_��� = "SELECT * FROM " + table_t + " WHERE status='1' AND account_nick='" + str(cst.account_nick) + "'"
    all_df = get_df_from_table(sql_all_���)
    if len(all_df) == 0:
        print(f"{��������} ��Ч���Ϊ�գ�����")

    for index, row in all_df.iterrows():
        qmt_code = row['qmt_code']
        name = qu.get_name_by_qmtcode(ContextInfo, qmt_code)

        ��t���� = get_num_by_numfield(row, '��t����')
        ��tֹ����� = get_num_by_numfield(row, '��tֹ�����')
        �Ƿ���t = (get_num_by_numfield(row, '�Ƿ���t') == 1)  # �˹����أ������Ƿ���Ҫ��t
        ���ھ��߰ٷֱ����� = get_num_by_numfield(row, '���ھ��߰ٷֱ�����')  # ��5������ʾ���ھ���5%����
        ���ھ��߰ٷֱ����� = get_num_by_numfield(row, '���ھ��߰ٷֱ�����')  # ��5������ʾ���ھ���5%����
        ��t�ʽ� = get_num_by_numfield(row, '��t�ʽ�')  # ��ǰ��t֧����ʽ���
        rt_��ǰ��t״̬ = get_str_by_strfield(row, 'rt_��ǰ��t״̬')
        period = get_str_by_strfield(row, 'period')  # ����
        if period is None or period not in qu.period_list:
            log_and_send_im(f"{��������} {qmt_code}[{name}] period ���ô��󣬱���Ϊ��{qu.period_list} ����֮һ�����飬������T���Ժ��ԣ���")
            continue
        if ��t���� <= 1:
            log_and_send_im(f"{��������} {qmt_code}[{name}]  ��t�������ô��� ��t���ߣ�{��t����}�����飬������T���Ժ��ԣ���")
            continue
        if ��tֹ����� <= 1:
            log_and_send_im(f"{��������} {qmt_code}[{name}] ֹ��������ô��� ��tֹ����ߣ�{��tֹ�����}�����飬������T���Ժ��ԣ���")
            continue

        �۲���ʼ�� = str(row['�۲���ʼ��dtime'])[:10]
        print(f"{��������} �۲���ʼ��: {�۲���ʼ��}")
        if �۲���ʼ�� is None:
            log_and_send_im(f"{��������} {qmt_code}[{name}] �۲���ʼ��dtime ���ô������飬�������Ժ��ԣ���")
            continue
        elif �۲���ʼ�� > get_curr_date():
            print(f"{��������} �۲���ʼ��: {�۲���ʼ��} δ��������������")
            continue

        if �Ƿ���t:
            df = qu.get_quatation_by_params(ContextInfo, qmt_code, period, ��t����, ��tֹ�����)
            curr_data = df.iloc[-1]
            ��ǰ�۸� = curr_data['close']
            where_clause = " WHERE qmt_code='" + qmt_code + "' AND account_nick='" + cst.account_nick + "'"

            if ��tֹ����� < 1000 and curr_data['pre_close'] < curr_data['ma' + str(��tֹ�����)]:  # ֹ��(ֹ���������Ϊ1000������ʱ����ֹ��)
                # todo:  �������δ�ɽ��ĵ��ӣ��Ƿ�Ҫ��57��֮ǰ�ȳ���
                �������� = qu.get_��������_by_qmtcode(qmt_code)
                if �������� == 0:
                    key = qmt_code + "_" + get_curr_date() + "_zs"
                    if g_data.get(key) is None:
                        g_data.update({key: '1'})
                        log_and_send_im(f"{��������} {table_t} {qmt_code}[{name}] �ﵽֹ����������������������Ϊ 0�����˹���飡��")
                    continue
                �������� = 100  # todo����λ�������ڼ��ݶ�100��

                qu.he_sell_stock(ContextInfo, qmt_code, name, ��������, ��������)  # �˰�ť��

                save_or_update_by_sql("UPDATE " + table_t + " SET �Ƿ���t='0', status='0' " + where_clause)
                log_and_send_im(f"{��������} {table_t} {qmt_code}[{name}] �ﵽ��tֹ���������������µ���֣���")
                continue
            else:
                ��Ⱦ����Ƿ� = curr_data['��Ⱦ����Ƿ�']
                print(f"{��������} ��Ⱦ����Ƿ�: {��Ⱦ����Ƿ�}, ���ھ��߰ٷֱ������� {���ھ��߰ٷֱ�����}, rt_��ǰ��t״̬: {rt_��ǰ��t״̬}")
                if ��Ⱦ����Ƿ� >= ���ھ��߰ٷֱ����� > 0 and (rt_��ǰ��t״̬ == '' or rt_��ǰ��t״̬ == '�����'):  # ��T����������
                    �ֲֿ������� = qu.get_��������_by_qmtcode(qmt_code)
                    ��t�������� = int(��t�ʽ� / ��ǰ�۸� / 100) * 100
                    �������� = min(��t��������, �ֲֿ�������)  # ȡdb�еĵ�ǰ�ֹ�����ֲ��еĿ���������ȡ����С���Ǹ������� todo����ǰ�ֹ����߼���Ҫ�����޸ģ������ڼ��Ⱥ���
                    if �������� == 0:
                        ttl = 10 * 60  # 10����
                        log_and_send_im_with_ttl(f"{��������} {qmt_code}[{name}] �ﵽ��������������������Ϊ�㡣��t����������{��t��������}, �ֲֿ�������: {�ֲֿ�������}", ttl)
                        continue

                    �������� = 100  # todo: ��λ�������ڼ��ݶ�100��

                    qu.he_sell_stock(ContextInfo, qmt_code, name, ��������, ��������)  # �˰�ť��

                    t_status = T_Type.��t��.value
                    update_sql = "UPDATE " + table_t + " SET rt_��ǰ��t״̬='" + t_status + "', rt_��ǰ�ֹ���='" + str(0) + "' " + where_clause
                    save_or_update_by_sql(update_sql)
                    continue

                t��ȫ���ɽ� = qu.check_ί���Ƿ���ȫ���ɽ�(qmt_code)
                if t��ȫ���ɽ� and (��Ⱦ����Ƿ� <= -���ھ��߰ٷֱ����� < 0) and (rt_��ǰ��t״̬ == '' or rt_��ǰ��t״̬ == '��t��'):  # ��T�����������µ����
                    ������� = int(��t�ʽ� / ��ǰ�۸� / 100) * 100
                    if ������� < 100:
                        print(f"{��������} {qmt_code}[{name}] �ﵽ�������������������������һ�֡����������{�������}, ��t�ʽ�: {��t�ʽ�}")
                        continue
                    ������� = 100  # todo: ��λ�������ڼ��ݶ�100��

                    qu.he_buy_stock(ContextInfo, qmt_code, name, �������, ��������)  # �˰�ť��

                    t_status = T_Type.�����.value
                    update_sql = "UPDATE " + table_t + " SET rt_��ǰ��t״̬='" + t_status + "', rt_��ǰ�ֹ���='" + str(�������) + "' " + where_clause
                    save_or_update_by_sql(update_sql)
                    continue
        else:
            rt_��Ӱ��dtime = get_dtime_by_datefield(row, 'rt_��Ӱ��dtime')
            rt_��Ӱ�ߺ��Ѵ������� = get_num_by_numfield(row, 'rt_��Ӱ�ߺ��Ѵ�������')
            ����ֹ����� = get_num_by_numfield(row, '����ֹ�����')

            df = qu.get_quatation_by_params(ContextInfo, qmt_code, period, ����ֹ�����)
            curr_data = df.iloc[-1]
            ��ǰ�۸� = curr_data['close']
            where_clause = " WHERE qmt_code='" + qmt_code + "' AND account_nick='" + cst.account_nick + "'"

            if curr_data['pre_close'] < curr_data['ma' + str(����ֹ�����)]:  # ֹ��
                �ֲֿ������� = qu.get_��������_by_qmtcode(qmt_code)
                db�������� = get_num_by_numfield(row, '����ֹ���������������')
                �������� = min(db��������, �ֲֿ�������)
                if �������� == 0:
                    ttl = 10 * 60  # 10����
                    log_and_send_im_with_ttl(f"{��������} {qmt_code}[{name}] �ﵽ��������������������Ϊ�㡣db����������{db��������}, �ֲֿ�������: {�ֲֿ�������}", ttl)
                    continue

                �������� = 100  # todo: ����Ϊ100��

                qu.he_sell_stock(ContextInfo, qmt_code, name, ��������, ��������)  # �˰�ť��

                save_or_update_by_sql("UPDATE " + table_t + " SET status='0' " + where_clause)
                log_and_send_im(f"{��������} {table_t} {qmt_code}[{name}] �ﵽֹ���������������µ���֣���")
            else:
                if rt_��Ӱ��dtime is not None and rt_��Ӱ�ߺ��Ѵ������� == 0:  # ����Ӱ������۸��98%�µ�
                    rt_��Ӱ�ߺ������۸� = get_num_by_numfield(row, 'rt_��Ӱ�ߺ������۸�')
                    if ��ǰ�۸� >= rt_��Ӱ�ߺ������۸�:
                        ��Ӱ�ߺ����������� = get_num_by_numfield(row, '��Ӱ�ߺ�����������')
                        �ֲֿ������� = qu.get_��������_by_qmtcode(qmt_code)
                        �������� = min(��Ӱ�ߺ�����������, �ֲֿ�������)
                        if �������� == 0:
                            ttl = 10 * 60  # 10����
                            log_and_send_im_with_ttl(f"{��������} {qmt_code}[{name}] �ﵽ��������������������Ϊ�㡣��Ӱ�ߺ�������������{��Ӱ�ߺ�����������}, �ֲֿ�������: {�ֲֿ�������}", ttl)
                            continue

                        �������� = 100  # todo: ����

                        qu.he_sell_stock(ContextInfo, qmt_code, name, ��������, ��������)  # �˰�ť��
                        save_or_update_by_sql("UPDATE " + table_t + " SET rt_��Ӱ�ߺ��Ѵ�������='1' WHERE qmt_code='" + qmt_code + "'")


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
