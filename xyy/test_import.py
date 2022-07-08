# encoding:gbk

from bsea_utils.bsea_xyy_util import *

策略名称 = "TEST_IMPORT"


def init(ContextInfo):
    print(f"-------------->>>>>> {策略名称} 策略已启动init")

    ContextInfo.set_account(cst.account)
    # 可用资金, 持仓df, obj_list =  get_stock_持仓列表(account)
    # print(可用资金)
    # orderid = get_last_order_id(account, 'stock', 'order')
    # print(orderid)

    # obj = get_value_by_order_id(orderid, account, 'stock', 'order')
    # print(obj)
    # print(obj.m_strInstrumentID)

    obj_list = get_trade_detail_data(cst.account, 'stock', 'ORDER')  # 委托
    print(f"帐户委托股票为：{obj_list}")
    order_id = ''
    for obj in obj_list:
        委托日期 = obj.m_strInsertDate
        委托时间 = obj.m_strInsertTime
        委托dtime = f"{委托日期[:4]}-{委托日期[4:6]}-{委托日期[6:8]} {委托时间[0:2]}:{委托时间[2:4]}:{委托时间[4:6]}"
        order_id = obj.m_strOrderSysID
        print(
            "\t证券代码：", obj.m_strInstrumentID, "\t证券名称：", obj.m_strInstrumentName,
            "\t最初委托量：", obj.m_nVolumeTotalOriginal, "\t剩余委托量：", obj.m_nVolumeTotal,
            "\t委托时间：", 委托dtime, "\t委托价格：", obj.m_dLimitPrice,
            "\t买卖标记：", obj.m_strOptName, "\t已撤数量：", obj.m_dCancelAmount,
            "\t成交均价：", obj.m_dTradedPrice, "\t成交金额：", obj.m_dTradeAmountRMB,
            "\t内部委托号：", obj.m_strOrderRef, "\t价格类型：", obj.m_nOrderPriceType,
            "\t委托号：", obj.m_strOrderSysID, "\t废单原因：", obj.m_strCancelInfo,
            "\tuser_order_mark：", obj.m_strRemark,

        )
    orderid = get_last_order_id(cst.account, 'stock', 'order')
    print(orderid)

    obj = get_value_by_order_id(orderid, cst.account, 'stock', 'order')
    print(f"{obj.m_strInstrumentID} {obj.m_strInstrumentName}")

    # if order_id != '':
    #     cancel(order_id, cst.account, 'STOCK', ContextInfo)


def handlebar(ContextInfo):
    print('这是 handlebar 中的 3秒一次的tick ~~~')


def stop(ContextInfo):
    log_and_send_im(f"------$$$$$$ {get_curr_date()}  {get_curr_time()}  {策略名称} 策略已停止！")
