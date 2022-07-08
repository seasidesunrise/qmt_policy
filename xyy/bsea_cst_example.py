"""
 
:Author:  逍遥游
:Create:  2022/6/30$ 16:43$
"""

from sqlalchemy import *
from sqlalchemy.orm import *

# account
account = ''
account_nick = ''

# 飞书
headers = {'Content-Type': 'application/json'}  # 定义数据类型
webhook = ''  # 定义webhook，从飞书群机器人设置页面复制获得


# db
mysql_url = ""
engine = create_engine(mysql_url, encoding='utf-8')
DBSession = sessionmaker(bind=engine)
session = DBSession()
