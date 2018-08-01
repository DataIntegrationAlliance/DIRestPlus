#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/7/24 15:49
@File    : ifind_rest_service.py
@contact : mmmaaaggg@163.com
@desc    :
"""
from direstplus import api
from flask_restplus import Resource, reqparse
import pandas as pd
import logging
from datetime import datetime, date
from direstplus.exceptions import RequestError
import iFinDPy as ifind
from direstplus.config import config
logger = logging.getLogger(__name__)
STR_FORMAT_DATE = '%Y-%m-%d'
STR_FORMAT_DATETIME_WIND = '%Y-%m-%d %H:%M:%S'  # 2017-03-06 00:00:00
UN_AVAILABLE_DATETIME = datetime.strptime('1900-01-01', STR_FORMAT_DATE)
UN_AVAILABLE_DATE = UN_AVAILABLE_DATETIME.date()

header = {'Content-Type': 'application/json'}
rec = api.namespace('iFind', description='同花顺iFind接口')

# parser
data_serial_parser = reqparse.RequestParser().add_argument(
    'thscode', type=str, help="同花顺代码，可以是单个代码也可以是多个代码，代码之间用逗号(‘,’)隔开。例如 600004.SH,600007.SH"
).add_argument(
    'jsonIndicator', type=str, help="指标，可以是单个指标也可以是多个指标，指标指标用 分号(‘;’)隔开。例如 ths_close_price_stock;ths_open_price_stock"
).add_argument(
    'jsonparam', type=str,
    help="参数，可以是默认参数也根据说明可以对参数进行自定义赋值，参数和参数之间用逗号 (‘ , ’) 隔开， 参 数 的 赋 值 用 冒 号 (‘:’) 。 例 如 100;100。复权方式：100-不复权 101-后复权 102-前复权 103-全流通后复权 104-全流通前复权"
).add_argument(
    'globalparam', type=str,
    help="参数，可以是默认参数也根据说明可以对参数进行自定义赋值，参数和参数之间用逗号 (‘, ’) 隔开， 参 数 的 赋 值 用 冒 号 (‘:’) 。 例 如 Days:Tradedays,Fill:Previous,Interval:D"
).add_argument(
    'begintime', type=str, help="开始时间，时间格式为 YYYY-MM-DD，例如 2018-06-24"
).add_argument(
    'endtime', type=str, help="截止时间，时间格式为 YYYY-MM-DD，例如 2018-07-24"
)

high_frequence_sequence_parser = reqparse.RequestParser().add_argument(
    'thscode', type=str, help="同花顺代码，可以是单个代码也可以是多个代码，代码之间用逗号(‘,’)隔开。例如 600004.SH,600007.SH"
).add_argument(
    'jsonIndicator', type=str, help="指标，可以是单个指标也可以是多个指标，指标之间用分号(‘;’)隔开。例如'close;open'"
).add_argument(
    'jsonparam', type=str, help="参数，可以是默认参数也可以根据说明对参数进行自定义赋值，参数和参数之间用逗号(‘，’)隔开，参数的赋值用冒号(‘:’)。例如'CPS:0,MaxPoints:50000'"
).add_argument(
    'begintime', type=str, help="开始时间，时间格式为YYYY-MM-DD HH:MM:SS，例如2018-05-15 09:30:00"
).add_argument(
    'endtime', type=str, help="截止时间，时间格式为YYYY-MM-DD HH:MM:SS，例如2018-05-15 10:00:00"
)

realtime_quotes_parser = reqparse.RequestParser().add_argument(
    'thscode', type=str, help="同花顺代码，可以是单个代码也可以是多个代码，代码之间用逗号(‘,’)隔开。例如 600004.SH,600007.SH"
).add_argument(
    'jsonIndicator', type=str, help="指标，可以是单个指标也可以是多个指标，指标之间用分号(‘;’)隔开。例如'close;open'"
).add_argument(
    'jsonparam', type=str, help="参数，可以是默认参数也可以根据说明对参数进行自定义赋值，参数和参数之间用逗号(‘，’)隔开，参数的赋值用冒号(‘:’)。例如'pricetype:1'"
)

history_quotes_parser = reqparse.RequestParser().add_argument(
    'thscode', type=str, help="同花顺代码，可以是单个代码也可以是多个代码，代码之间用逗号(‘,’)隔开。例如 600004.SH,600007.SH"
).add_argument(
    'jsonIndicator', type=str, help="指标，可以是单个指标也可以是多个指标，指标之间用分号(‘;’)隔开。例如'close;open'"
).add_argument(
    'jsonparam', type=str, help="参数，可以是默认参数也根据说明可以对参数进行自定义赋值，参数和参数之间用逗号(‘，’)隔开，参数的赋值用冒号(‘:’)。例如'period:D,pricetype:1,rptcategory:1'"
).add_argument(
    'begintime', type=str, help="开始时间，时间格式为YYYY-MM-DD，例如2015-06-23"
).add_argument(
    'endtime', type=str, help="截止时间，时间格式为YYYY-MM-DD，例如2016-06-23"
)

snap_shot_parser = reqparse.RequestParser().add_argument(
    'thscode', type=str, help="同花顺代码，可以是单个代码也可以是多个代码，代码之间用逗号(‘,’)隔开。例如 600004.SH,600007.SH"
).add_argument(
    'jsonIndicator', type=str, help="指标，可以是单个指标也可以是多个指标，指标之间用分号(‘;’)隔开。例如'close;open'"
).add_argument(
    'jsonparam', type=str, help="参数，当前参数只能是dataType:Original"
).add_argument(
    'begintime', type=str, help="开始时间，时间格式为YYYY-MM-DD HH:MM:SS，例如2017-05-15 09:30:00。"
).add_argument(
    'endtime', type=str, help="截止时间，时间格式为YYYY-MM-DD HH:MM:SS，例如2017-05-15 10:00:00"
)

basic_data_parser = reqparse.RequestParser().add_argument(
    'thsCode', type=str, help="同花顺代码，可以是单个代码也可以是多个代码，代码之间用逗号(‘,’)隔开。例如'300033.SZ,600000.SH'"
).add_argument(
    'indicatorName', type=str, help="指标，可以是单个指标也可以是多个指标，指标之间用分号(‘;’)隔开。例如'ths_stock_short_name_stock;ths_np_stock'"
).add_argument(
    'paramOption', type=str, help="函数对应的参数，参数和参数之间用逗号(‘，’)隔开。例如';2017-12-31,100'"
)

data_pool_parser = reqparse.RequestParser().add_argument(
    'DataPoolname', type=str, help="数据池名称(必填) 例如:block"
).add_argument(
    'paramname', type=str, help="输入参数，参数和参数之间使用分号(‘;’)隔开，如'2018-05-31;001005334008'"
).add_argument(
    'FunOption', type=str, help="输出参数，参数和参数之间使用逗号(‘,’)隔开，如'date:Y, security_name:Y, thscode:Y'，其中“Y”表示输出，“N”表示不输出"
)

quote_push_parser = reqparse.RequestParser().add_argument(
    'thscode', type=str, help="同花顺代码，可以是单个代码也可以是多个代码，代码之间用逗号(‘,’)隔开。例如'300033.SZ,600000.SH,600004.SH'"
).add_argument(
    'indicator', type=str, help="指标，可以是单个指标也可以是多个指标，指标指标用分号(‘;’)隔开。例如'close;open'，指标可以为空"
)

edb_query_parser = reqparse.RequestParser().add_argument(
    'indicators', type=str, help="EDB指标ID，可以是单个代码也可以是多个代码，代码之间用逗号(‘,’)隔开。例如'M001620326,M002822183'"
).add_argument(
    'begintime', type=str, help="开始时间，时间格式为YYYY-MM-DD，例如2015-06-23"
).add_argument(
    'endtime', type=str, help="截止时间，时间格式为YYYY-MM-DD，例如2016-06-23"
)

date_query_parser = reqparse.RequestParser().add_argument(
    'exchange', type=str, help="交易所简称。例如 SSE"
).add_argument(
    'params', type=str, help="参数，可选择日期类型，时间频率，日期格式。例如：'dateType:0,period:D,dateFormat:0'"
).add_argument(
    'begintime', type=str, help="开始时间,例如2018-01-01"
).add_argument(
    'endtime', type=str, help="截止时间,例如2018-06-30"
)


def format_2_date_str(dt):
    if dt is None:
        return None
    dt_type = type(dt)
    if dt_type == str:
        return dt
    elif dt_type == date:
        if dt > UN_AVAILABLE_DATE:
            return dt.strftime(STR_FORMAT_DATE)
        else:
            return None
    elif dt_type == datetime:
        if dt > UN_AVAILABLE_DATETIME:
            return dt.strftime(STR_FORMAT_DATE)
        else:
            return None
    else:
        return dt


def format_2_datetime_str(dt):
    if dt is None:
        return None
    dt_type = type(dt)
    if dt_type == str:
        return dt
    elif dt_type == date:
        if dt > UN_AVAILABLE_DATE:
            return dt.strftime(STR_FORMAT_DATE)
        else:
            return None
    elif dt_type == datetime:
        if dt > UN_AVAILABLE_DATETIME:
            return dt.strftime(STR_FORMAT_DATETIME_WIND)
        else:
            return None
    else:
        return dt


def ifind_login():
    ths_login = ifind.THS_iFinDLogin(config.THS_LOGIN_USER_NAME, config.THS_LOGIN_PASSWORD)
    if ths_login == 0 or ths_login == -201:
        logger.info('iFind 成功登陆')
    else:
        logger.error("iFind 登录失败")
    return ths_login


def ifind_logout():
    ths_logout = ifind.THS_iFinDLogout()
    logger.info('ifind 成功登出')
    return ths_logout


@rec.route('/THS_DateSerial/')
class THSDateSerial(Resource):

    @rec.expect(data_serial_parser)
    def post(self):
        """
        日期序列
        """
        # data_dic = request.json
        args = data_serial_parser.parse_args()
        logger.info('/THS_DateSerial/ args:%s' % args)
        for nth in range(2):
            ret_data = ifind.THS_DateSerial(**args)
            error_code = ret_data['errorcode']
            if error_code != 0:
                # 错误处理
                if error_code == -1010:
                    ths_login = ifind_login()
                    if ths_login == 0 or ths_login == -201:
                        logger.warning('尝试重新登陆成功，再次调用函数')
                        continue
                    else:
                        logger.error('尝试重新登陆失败')

                msg = ret_data['errmsg']
                logger.error('THS_DateSerial(%s) ErrorCode=%d %s' % (args, error_code, msg))
                raise RequestError(msg, None, error_code)
            else:
                break

        tables = ret_data['tables']
        table_count = len(tables)
        data_df_list = []
        for nth_table in range(table_count):
            table = tables[nth_table]
            if 'time' not in table:
                logger.error('%d/%d) 当前结果没有 time 列，将被跳过 %s', nth_table, table_count)
                continue
            date_list = table['time']
            date_len = len(date_list)
            if date_len > 0:
                data = table['table']
                data_df = pd.DataFrame(data, index=date_list)
                data_df['ths_code'] = table['thscode']
                data_df_list.append(data_df)

        # 若没有数据，返回空
        if len(data_df_list) == 0:
            return None

        ret_df = pd.concat(data_df_list)
        ret_df.index.rename('time', inplace=True)
        ret_df.reset_index(inplace=True)
        # print('ret_df\n', ret_df)
        ret_dic = ret_df.to_dict()
        # print('ret_dic:\n', ret_dic)
        return ret_dic


@rec.route('/THS_HighFrequenceSequence/')
class THSHighFrequenceSequence(Resource):

    @rec.expect(high_frequence_sequence_parser)
    def post(self):
        """
        高频序列
        """
        # data_dic = request.json
        args = high_frequence_sequence_parser.parse_args()
        logger.info('/THS_HighFrequenceSequence/ args:%s' % args)
        for nth in range(2):
            ret_data = ifind.THS_HighFrequenceSequence(**args)
            error_code = ret_data['errorcode']
            if error_code != 0:
                # 错误处理
                if error_code == -1010:
                    ths_login = ifind_login()
                    if ths_login == 0 or ths_login == -201:
                        logger.warning('尝试重新登陆成功，再次调用函数')
                        continue
                    else:
                        logger.error('尝试重新登陆失败')

                msg = ret_data['errmsg']
                logger.error('THS_HighFrequenceSequence(%s) ErrorCode=%d %s' % (args, error_code, msg))
                raise RequestError(msg, None, error_code)
            else:
                break

        tables = ret_data['tables']
        table_count = len(tables)
        data_df_list = []
        for nth_table in range(table_count):
            table = tables[nth_table]
            if 'time' not in table:
                logger.error('%d/%d) 当前结果没有 time 列，将被跳过 %s', nth_table, table_count)
                continue
            date_list = table['time']
            date_len = len(date_list)
            if date_len > 0:
                data = table['table']
                data_df = pd.DataFrame(data, index=date_list)
                data_df['ths_code'] = table['thscode']
                data_df_list.append(data_df)

        # 若没有数据，返回空
        if len(data_df_list) == 0:
            return None

        ret_df = pd.concat(data_df_list)
        ret_df.index.rename('time', inplace=True)
        ret_df.reset_index(inplace=True)
        # print('ret_df\n', ret_df)
        ret_dic = ret_df.to_dict()
        # print('ret_dic:\n', ret_dic)
        return ret_dic


@rec.route('/THS_RealtimeQuotes/')
class THSRealtimeQuotes(Resource):

    @rec.expect(realtime_quotes_parser)
    def post(self):
        """
        实时行情序列
        """
        # data_dic = request.json
        args = realtime_quotes_parser.parse_args()
        logger.info('/THS_RealtimeQuotes/ args:%s' % args)
        for nth in range(2):
            ret_data = ifind.THS_RealtimeQuotes(**args)
            error_code = ret_data['errorcode']
            if error_code != 0:
                # 错误处理
                if error_code == -1010:
                    ths_login = ifind_login()
                    if ths_login == 0 or ths_login == -201:
                        logger.warning('尝试重新登陆成功，再次调用函数')
                        continue
                    else:
                        logger.error('尝试重新登陆失败')

                msg = ret_data['errmsg']
                logger.error('THS_RealtimeQuotes(%s) ErrorCode=%d %s' % (args, error_code, msg))
                raise RequestError(msg, None, error_code)
            else:
                break

        tables = ret_data['tables']
        table_count = len(tables)
        data_df_list = []
        for nth_table in range(table_count):
            table = tables[nth_table]
            if 'time' not in table:
                logger.error('%d/%d) 当前结果没有 time 列，将被跳过 %s', nth_table, table_count)
                continue
            date_list = table['time']
            date_len = len(date_list)
            if date_len > 0:
                data = table['table']
                data_df = pd.DataFrame(data, index=date_list)
                data_df['ths_code'] = table['thscode']
                data_df_list.append(data_df)

        # 若没有数据，返回空
        if len(data_df_list) == 0:
            return None

        ret_df = pd.concat(data_df_list)
        ret_df.index.rename('time', inplace=True)
        ret_df.reset_index(inplace=True)
        # print('ret_df\n', ret_df)
        ret_dic = ret_df.to_dict()
        # print('ret_dic:\n', ret_dic)
        return ret_dic


@rec.route('/THS_HistoryQuotes/')
class THSHistoryQuotes(Resource):

    @rec.expect(history_quotes_parser)
    def post(self):
        """
        历史行情序列
        """
        # data_dic = request.json
        args = history_quotes_parser.parse_args()
        logger.info('/THS_HistoryQuotes/ args:%s' % args)
        for nth in range(2):
            ret_data = ifind.THS_HistoryQuotes(**args)
            error_code = ret_data['errorcode']
            if error_code != 0:
                # 错误处理
                if error_code == -1010:
                    ths_login = ifind_login()
                    if ths_login == 0 or ths_login == -201:
                        logger.warning('尝试重新登陆成功，再次调用函数')
                        continue
                    else:
                        logger.error('尝试重新登陆失败')

                msg = ret_data['errmsg']
                logger.error('THS_HistoryQuotes(%s) ErrorCode=%d %s' % (args, error_code, msg))
                raise RequestError(msg, None, error_code)
            else:
                break

        tables = ret_data['tables']
        table_count = len(tables)
        data_df_list = []
        for nth_table in range(table_count):
            table = tables[nth_table]
            if 'time' not in table:
                logger.error('%d/%d) 当前结果没有 time 列，将被跳过 %s', nth_table, table_count)
                continue
            date_list = table['time']
            date_len = len(date_list)
            if date_len > 0:
                data = table['table']
                data_df = pd.DataFrame(data, index=date_list)
                data_df['ths_code'] = table['thscode']
                data_df_list.append(data_df)

        # 若没有数据，返回空
        if len(data_df_list) == 0:
            return None

        ret_df = pd.concat(data_df_list)
        ret_df.index.rename('time', inplace=True)
        ret_df.reset_index(inplace=True)
        # print('ret_df\n', ret_df)
        ret_dic = ret_df.to_dict()
        # print('ret_dic:\n', ret_dic)
        return ret_dic


@rec.route('/THS_Snapshot/')
class THSSnapshot(Resource):

    @rec.expect(snap_shot_parser)
    def post(self):
        """
        日内快照序列
        """
        # data_dic = request.json
        args = snap_shot_parser.parse_args()
        logger.info('/THS_Snapshot/ args:%s' % args)
        for nth in range(2):
            ret_data = ifind.THS_Snapshot(**args)
            error_code = ret_data['errorcode']
            if error_code != 0:
                # 错误处理
                if error_code == -1010:
                    ths_login = ifind_login()
                    if ths_login == 0 or ths_login == -201:
                        logger.warning('尝试重新登陆成功，再次调用函数')
                        continue
                    else:
                        logger.error('尝试重新登陆失败')

                msg = ret_data['errmsg']
                logger.error('THS_Snapshot(%s) ErrorCode=%d %s' % (args, error_code, msg))
                raise RequestError(msg, None, error_code)
            else:
                break

        tables = ret_data['tables']
        table_count = len(tables)
        data_df_list = []
        for nth_table in range(table_count):
            table = tables[nth_table]
            if 'time' not in table:
                logger.error('%d/%d) 当前结果没有 time 列，将被跳过 %s', nth_table, table_count)
                continue
            date_list = table['time']
            date_len = len(date_list)
            if date_len > 0:
                data = table['table']
                data_df = pd.DataFrame(data, index=date_list)
                data_df['ths_code'] = table['thscode']
                data_df_list.append(data_df)

        # 若没有数据，返回空
        if len(data_df_list) == 0:
            return None

        ret_df = pd.concat(data_df_list)
        ret_df.index.rename('time', inplace=True)
        ret_df.reset_index(inplace=True)
        # print('ret_df\n', ret_df)
        ret_dic = ret_df.to_dict()
        # print('ret_dic:\n', ret_dic)
        return ret_dic


@rec.route('/THS_BasicData/')
class THSBasicData(Resource):

    @rec.expect(basic_data_parser)
    def post(self):
        """
        基础数据序列
        """
        # data_dic = request.json
        args = basic_data_parser.parse_args()
        logger.info('/THS_BasicData/ args:%s' % args)
        for nth in range(2):
            ret_data = ifind.THS_BasicData(**args)
            error_code = ret_data['errorcode']
            if error_code != 0:
                # 错误处理
                if error_code == -1010:
                    ths_login = ifind_login()
                    if ths_login == 0 or ths_login == -201:
                        logger.warning('尝试重新登陆成功，再次调用函数')
                        continue
                    else:
                        logger.error('尝试重新登陆失败')

                msg = ret_data['errmsg']
                logger.error('THS_BasicData(%s) ErrorCode=%d %s' % (args, error_code, msg))
                raise RequestError(msg, None, error_code)
            else:
                break

        tables = ret_data['tables']
        table_count = len(tables)
        if table_count == 0:
            return None
        data_df_list = []
        for nth_table in range(table_count):
            table = tables[nth_table]
            if 'table' not in table:
                logger.error('%d/%d) 当前结果没有 time 列，将被跳过 %s', nth_table, table_count)
                continue
            data = table['table']
            data_df = pd.DataFrame(data)
            data_df['ths_code'] = table['thscode']
            data_df_list.append(data_df)

        # 若没有数据，返回空
        if len(data_df_list) == 0:
            return None

        ret_df = pd.concat(data_df_list).reset_index(drop=True)
        # print('ret_df\n', ret_df)
        ret_dic = ret_df.to_dict()
        # print('ret_dic:\n', ret_dic)
        return ret_dic


@rec.route('/THS_DataPool/')
class THSDataPool(Resource):

    @rec.expect(data_pool_parser)
    def post(self):
        """
        数据池序列
        """
        # data_dic = request.json
        args = data_pool_parser.parse_args()
        logger.info('/THS_DataPool/ args:%s' % args)
        for nth in range(2):
            ret_data = ifind.THS_DataPool(**args)
            error_code = ret_data['errorcode']
            if error_code != 0:
                if error_code == -1010:
                    ths_login = ifind_login()
                    if ths_login == 0 or ths_login == -201:
                        logger.warning('尝试重新登陆成功，再次调用函数')
                        continue
                    else:
                        logger.error('尝试重新登陆失败')

                msg = ret_data['errmsg']
                logger.error('THS_DataPool(%s) ErrorCode=%d %s' % (args, error_code, msg))
                raise RequestError(msg, None, error_code)
            else:
                break

        tables = ret_data['tables']
        table_count = len(tables)
        data_df_list = []
        if table_count > 0:
            for nth_table in range(table_count):
                table = tables[nth_table]
                if 'table' not in table:
                    logger.error('%d/%d) 当前结果没有 time 列，将被跳过 %s', nth_table, table_count)
                    continue
                data = table['table']
                data_df = pd.DataFrame(data)
                if table['thscode'] != '':
                    data_df['ths_code'] = table['thscode']
                data_df_list.append(data_df)

        # 若没有数据，返回空
        if len(data_df_list) == 0:
            return None

        ret_df = pd.concat(data_df_list)
        ret_df.index = [str(idx) for idx in ret_df.index]
        # print('ret_df\n', ret_df)
        ret_dic = ret_df.to_dict()
        # print('ret_dic:\n', ret_dic)
        return ret_dic


# @rec.route('/THS_QuotesPushing/')
# class THSQuotesPushing(Resource):
#
#     @rec.expect(quote_push_parser)
#     def post(self):
#         """
#         实时行情推送序列
#         """
#         # data_dic = request.json
#         args = quote_push_parser.parse_args()
#         logger.info('/THS_QuotesPushing/ args:%s' % args)
#         ret_data = ifind.THS_QuotesPushing(**args)
#         error_code = ret_data['errorcode']
#         if error_code != 0:
#             msg = ret_data['errmsg']
#             logger.error('THS_QuotesPushing(%s) ErrorCode=%d %s' % (args, error_code, msg))
#             raise RequestError(msg, None, error_code)
#
#         tables = ret_data['tables']
#         table_count = len(tables)
#         data_df_list = []
#         print(table_count)
#         if table_count > 0:
#             for nth_table in range(table_count):
#                 table = tables[nth_table]
#                 data = table['table']
#                 data_df = pd.DataFrame(data)
#                 data_df['ths_code'] = table['thscode']
#                 data_df_list.append(data_df)
#         ret_df = pd.concat(data_df_list)
#         # print('ret_df\n', ret_df)
#         ret_dic = ret_df.to_dict()
#         # print('ret_dic:\n', ret_dic)
#         return ret_dic


@rec.route('/THS_EDBQuery/')
class THSEDBQuery(Resource):

    @rec.expect(edb_query_parser)
    def post(self):
        """
        EDB查询序列
        """
        # data_dic = request.json
        args = edb_query_parser.parse_args()
        logger.info('/THS_EDBQuery/ args:%s' % args)
        for nth in range(2):
            ret_data = ifind.THS_EDBQuery(**args)
            error_code = ret_data['errorcode']
            if error_code != 0:
                # 错误处理
                if error_code == -1010:
                    ths_login = ifind_login()
                    if ths_login == 0 or ths_login == -201:
                        logger.warning('尝试重新登陆成功，再次调用函数')
                        continue
                    else:
                        logger.error('尝试重新登陆失败')

                msg = ret_data['errmsg']
                logger.error('THS_EDBQuery(%s) ErrorCode=%d %s' % (args, error_code, msg))
                raise RequestError(msg, None, error_code)
            else:
                break

        tables = ret_data['tables']
        table_count = len(tables)
        data_df_list = []
        if table_count > 0:
            for nth_table in range(table_count):
                table = tables[nth_table]
                if 'id' not in table:
                    logger.error('%d/%d) 当前结果没有 time 列，将被跳过 %s', nth_table, table_count)
                    continue
                if len(table['id']) > 0:
                    temp = table['id']
                    table.pop('id')
                    tab = pd.DataFrame(table)
                    tab['id'] = temp[0]
                    data_df_list.append(tab)
                else:
                    data_df = pd.DataFrame(table)
                    data_df_list.append(data_df)

        # 若没有数据，返回空
        if len(data_df_list) == 0:
            return None

        ret_df = pd.concat(data_df_list)
        # print('ret_df\n', ret_df)
        ret_dic = ret_df.to_dict()
        # print('ret_dic:\n', ret_dic)
        return ret_dic


@rec.route('/THS_DateQuery/')
class THSDateQuery(Resource):

    @rec.expect(date_query_parser)
    def post(self):
        """
        日期查询序列
        """
        # data_dic = request.json
        args = date_query_parser.parse_args()
        logger.info('/THS_DateQuery/ args:%s' % args)
        for nth in range(2):
            ret_data = ifind.THS_DateQuery(**args)
            error_code = ret_data['errorcode']
            if error_code != 0:
                # 错误处理
                if error_code == -1010:
                    ths_login = ifind_login()
                    if ths_login == 0 or ths_login == -201:
                        logger.warning('尝试重新登陆成功，再次调用函数')
                        continue
                    else:
                        logger.error('尝试重新登陆失败')

                msg = ret_data['errmsg']
                logger.error('THS_DateQuery(%s) ErrorCode=%d %s' % (args, error_code, msg))
                raise RequestError(msg, None, error_code)
            else:
                break

        tables = ret_data['tables']
        table_count = len(tables)
        data_df_list = []

        # 若没有数据，返回空
        if table_count == 0:
            return None

        data_df = pd.DataFrame(tables)
        data_df_list.append(data_df)
        ret_df = pd.concat(data_df_list)
        # print('ret_df\n', ret_df)
        ret_dic = ret_df.to_dict()
        # print('ret_dic:\n', ret_dic)
        return ret_dic
