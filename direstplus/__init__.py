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
from flask_restplus._http import HTTPStatus
from werkzeug.exceptions import BadRequest
from direstplus.exceptions import RequestError
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


@api.errorhandler(RequestError)
def login_error_handler(error: RequestError):
    # logger.error('error on login| %s', error.description)
    return {'status': 'error',
            'message': error.description,
            'error_name': error.__class__.__name__,
            'error_code': error.error_code,
            }, error.code


@api.errorhandler(Exception)
def login_error_handler(error: Exception):
    """仅作为一个异常处理的例子"""
    # logger.error('error on login| %s', error.description)
    if isinstance(error, RequestError):
        return {'status': 'error',
                'message': error.description,
                'error_name': error.__class__.__name__,
                'error_code': error.error_code,
                }, error.code
    elif isinstance(error, BadRequest):
        return {'status': 'error',
                'message': error.description,
                'error_name': error.__class__.__name__,
                }, error.code
    else:
        return {'status': 'error',
                'message': error.args[0],
                'error_name': error.__class__.__name__,
                }, HTTPStatus.BAD_REQUEST


# 加载 iFinD 接口
if config.ENABLE_IFIND:
    try:
        import iFinDPy
        from direstplus.ifind import *
        logger.info('加载 iFinD 接口')
    except ImportError:
        config.ENABLE_IFIND = False

# 加载 Wind 接口
if config.ENABLE_WIND:
    try:
        import WindPy
        from direstplus.wind import *
        logger.info('加载 Wind 接口')
    except ImportError:
        config.ENABLE_WIND = False


def start_service():
    """启动RESTPlus服务"""

    if config.ENABLE_IFIND:
        ths_login = ifind_login()

    if config.ENABLE_WIND:
        try:
            if not WindPy.w.isconnected():
                WindPy.w.start()
                logger.info('Wind 成功登陆')
        except:
            logger.error("Wind 登录失败")

    try:
        from direstplus import app
        app.run(host="0.0.0.0", debug=True, use_reloader=False)
    finally:
        if config.ENABLE_IFIND:
            ifind_logout()

        if config.ENABLE_WIND:
            WindPy.w.close()
            logger.info('Wind 成功登出')


if __name__ == '__main__':
    start_service()
