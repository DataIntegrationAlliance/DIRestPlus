#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/3/30 17:53
@File    : config.py
@contact : mmmaaaggg@163.com
@desc    :
"""
import logging
from logging.config import dictConfig


# Use a Class-based config to avoid needing a 2nd file
# os.getenv() enables configuration through OS environment variables
class ConfigBase(object):
    # 同花顺账号密码 settings
    THS_LOGIN_USER_NAME = '***'
    THS_LOGIN_PASSWORD = '***'
    # log settings
    logging_config = dict(
        version=1,
        formatters={
            'simple': {
                'format': '%(asctime)s %(name)s|%(module)s.%(funcName)s:%(lineno)d %(levelname)s %(message)s'}
        },
        handlers={
            'file_handler':
                {
                    'class': 'logging.handlers.RotatingFileHandler',
                    'filename': 'logger.log',
                    'maxBytes': 1024 * 1024 * 10,
                    'backupCount': 5,
                    'level': 'DEBUG',
                    'formatter': 'simple',
                    'encoding': 'utf8'
                },
            'console_handler':
                {
                    'class': 'logging.StreamHandler',
                    'level': 'DEBUG',
                    'formatter': 'simple'
                }
        },

        root={
            'handlers': ['console_handler'],  # , 'file_handler'
            'level': logging.DEBUG,
        }
    )
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARN)
    dictConfig(logging_config)


config = ConfigBase()
