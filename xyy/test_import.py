# encoding:gbk

from bsea_utils.bsea_xyy_util import *

�������� = "TEST_IMPORT"


def init(ContextInfo):
    print(f"-------------->>>>>> {��������} ����������init")

    ContextInfo.set_account(cst.account)
    # �����ʽ�, �ֲ�df, obj_list =  get_stock_�ֲ��б�(account)
    # print(�����ʽ�)
    # orderid = get_last_order_id(account, 'stock', 'order')
    # print(orderid)

    # obj = get_value_by_order_id(orderid, account, 'stock', 'order')
    # print(obj)
    # print(obj.m_strInstrumentID)

    obj_list = get_trade_detail_data(cst.account, 'stock', 'ORDER')  # ί��
    print(f"�ʻ�ί�й�ƱΪ��{obj_list}")
    order_id = ''
    for obj in obj_list:
        ί������ = obj.m_strInsertDate
        ί��ʱ�� = obj.m_strInsertTime
        ί��dtime = f"{ί������[:4]}-{ί������[4:6]}-{ί������[6:8]} {ί��ʱ��[0:2]}:{ί��ʱ��[2:4]}:{ί��ʱ��[4:6]}"
        order_id = obj.m_strOrderSysID
        print(
            "\t֤ȯ���룺", obj.m_strInstrumentID, "\t֤ȯ���ƣ�", obj.m_strInstrumentName,
            "\t���ί������", obj.m_nVolumeTotalOriginal, "\tʣ��ί������", obj.m_nVolumeTotal,
            "\tί��ʱ�䣺", ί��dtime, "\tί�м۸�", obj.m_dLimitPrice,
            "\t������ǣ�", obj.m_strOptName, "\t�ѳ�������", obj.m_dCancelAmount,
            "\t�ɽ����ۣ�", obj.m_dTradedPrice, "\t�ɽ���", obj.m_dTradeAmountRMB,
            "\t�ڲ�ί�кţ�", obj.m_strOrderRef, "\t�۸����ͣ�", obj.m_nOrderPriceType,
            "\tί�кţ�", obj.m_strOrderSysID, "\t�ϵ�ԭ��", obj.m_strCancelInfo,
            "\tuser_order_mark��", obj.m_strRemark,

        )
    orderid = get_last_order_id(cst.account, 'stock', 'order')
    print(orderid)

    obj = get_value_by_order_id(orderid, cst.account, 'stock', 'order')
    print(f"{obj.m_strInstrumentID} {obj.m_strInstrumentName}")

    # if order_id != '':
    #     cancel(order_id, cst.account, 'STOCK', ContextInfo)


def handlebar(ContextInfo):
    print('���� handlebar �е� 3��һ�ε�tick ~~~')


def stop(ContextInfo):
    log_and_send_im(f"------$$$$$$ {get_curr_date()}  {get_curr_time()}  {��������} ������ֹͣ��")
