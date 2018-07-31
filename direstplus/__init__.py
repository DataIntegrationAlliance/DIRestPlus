#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/7/29 13:06
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from flask import Flask
from flask_restplus import Api
import logging
from datetime import datetime
from direstplus.config import config

logger = logging.getLogger(__name__)
STR_FORMAT_DATE = '%Y-%m-%d'
STR_FORMAT_DATETIME_WIND = '%Y-%m-%d %H:%M:%S'  # 2017-03-06 00:00:00
UN_AVAILABLE_DATETIME = datetime.strptime('1900-01-01', STR_FORMAT_DATE)
UN_AVAILABLE_DATE = UN_AVAILABLE_DATETIME.date()
app = Flask(__name__)
api = Api(app,
          title='Data Integration RestPlus API',
          version='0.0.1',
          description='Wind、iFinD、Choice 接口封装API',
          )

# 加载 iFinD 接口
has_ifind_api = True
try:
    import iFinDPy
    from direstplus.ifind import *
    logger.info('加载 iFinD 接口')
except ImportError:
    has_ifind_api = False

# 加载 Wind 接口
has_wind_api = True
try:
    import WindPy
    from direstplus.wind import *
    logger.info('加载 Wind 接口')
except ImportError:
    has_wind_api = False


def start_service():
    """启动RESTPlus服务"""

    if has_ifind_api:
        ths_login = ifind.THS_iFinDLogin(config.THS_LOGIN_USER_NAME, config.THS_LOGIN_PASSWORD)
        if ths_login == 0 or ths_login == -201:
            logger.info('iFind 成功登陆')
        else:
            logger.error("iFind 登录失败")

    if has_wind_api:
        if WindPy.w.isconnected():
            WindPy.w.start()
            logger.info('Wind 成功登陆')
        else:
            logger.error("Wind 登录失败")

    try:
        from direstplus import app
        app.run(host="0.0.0.0", debug=True)
    finally:
        if has_ifind_api:
            ifind.THS_iFinDLogout()
            logger.info('ifind 成功登出')

        if has_wind_api:
            WindPy.w.close()
            logger.info('Wind 成功登出')


if __name__ == '__main__':
    start_service()
