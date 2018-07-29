#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/7/13 15:25
@File    : exceptions.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from werkzeug.exceptions import BadRequest


class RequestError(BadRequest):

    def __init__(self, description=None, response=None, error_code=None):
        super().__init__(description, response)
        self.errcode = error_code
