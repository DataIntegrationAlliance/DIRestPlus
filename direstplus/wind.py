# -*- coding: utf-8 -*-
"""
Created on 2016-12-22
@author: MG
"""
from direstplus import api
from flask_restplus import Resource, reqparse
from WindPy import w
import pandas as pd
import logging
from datetime import datetime, date
from direstplus.exceptions import RequestError

logger = logging.getLogger(__name__)
STR_FORMAT_DATE = '%Y-%m-%d'
STR_FORMAT_DATETIME_WIND = '%Y-%m-%d %H:%M:%S'  # 2017-03-06 00:00:00
UN_AVAILABLE_DATETIME = datetime.strptime('1900-01-01', STR_FORMAT_DATE)
UN_AVAILABLE_DATE = UN_AVAILABLE_DATETIME.date()

header = {'Content-Type': 'application/json'}
rec = api.namespace('wind', description='wind接口')

ERROR_CODE_MSG_DIC = {
    -40522005: "不支持的万得代码",
    -40522003: "非法请求",
    -40521004: "请求发送失败。无法发送请求，请连接网络",
    -40520007: "没有可用数据",
    -40521009: "数据解码失败。检查输入参数是否正确，如：日期参数注意大小月月末及短二月",
    -40521010: "网络超时",
    -40522017: "数据提取量超限",
    -40522006: "指标语法错误。请检查代码中的相关指标是否正确，无缺失或重复",
    -40520004: "登陆失败，请确保Wind终端和编程程序以相同的windows权限启动",
    -40520008: "Start登陆超时，请重新登陆",
}

# parser
receive_wset_parser = reqparse.RequestParser().add_argument(
    'tablename', type=str, required=True, help="数据集名称"
).add_argument(
    'options', type=str, help="可选参数"
)

receive_wsd_parser = reqparse.RequestParser().add_argument(
    'codes', type=str, required=True, help="数据集名称"
).add_argument(
    'fields', type=str, help="指标"
).add_argument(
    'beginTime', type=str, help="开始时间"
).add_argument(
    'endTime', type=str, help="截止时间"
).add_argument(
    'options', type=str, help="可选参数"
)

receive_wsi_parser = reqparse.RequestParser().add_argument(
    'codes', type=str, required=True, help="数据集名称"
).add_argument(
    'fields', type=str, help="指标"
).add_argument(
    'beginTime', type=str, help="开始时间"
).add_argument(
    'endTime', type=str, help="截止时间"
).add_argument(
    'options', type=str, help="可选参数"
)

receive_wss_parser = reqparse.RequestParser().add_argument(
    'codes', type=str, required=True, help="数据集名称"
).add_argument(
    'fields', type=str, help="指标"
).add_argument(
    'options', type=str, help="可选参数"
)

tdays_offset_parser = reqparse.RequestParser().add_argument(
    'offsets', type=str, required=True, help="偏移值"
).add_argument(
    'beginTime', type=str, help="基准时间"
).add_argument(
    'options', type=str, help="可选参数"
)

tdays_parser = reqparse.RequestParser().add_argument(
    'beginTime', type=str, help="开始时间"
).add_argument(
    'endTime', type=str, help="结束时间"
).add_argument(
    'options', type=str, help="可选参数"
)

receive_wsq_parser = reqparse.RequestParser().add_argument(
    'codes', type=str, required=True, help="数据集名称"
).add_argument(
    'fields', type=str, help="指标"
).add_argument(
    'options', type=str, help="可选参数"
)

receive_wst_parser = reqparse.RequestParser().add_argument(
    'codes', type=str, required=True, help="数据集名称"
).add_argument(
    'fields', type=str, help="指标"
).add_argument(
    'beginTime', type=str, help="开始时间"
).add_argument(
    'endTime', type=str, help="截止时间"
).add_argument(
    'options', type=str, help="可选参数"
)

receive_edb_parser = reqparse.RequestParser().add_argument(
    'codes', type=str, required=True, help="数据集名称"
).add_argument(
    'beginTime', type=str, help="开始时间"
).add_argument(
    'endTime', type=str, help="截止时间"
).add_argument(
    'options', type=str, help="可选参数"
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


@rec.route('/wset/')
class ReceiveWSET(Resource):

    @rec.expect(receive_wset_parser)
    def post(self):
        """
        json str:{"tablename": "sectorconstituent", "options": "date=2017-03-21;sectorid=1000023121000000"}
        :return: 返回万得返回数据dict
        """
        args = receive_wset_parser.parse_args()
        logger.info('/wset/ args:%s' % args)
        # print('args:%s' % args)
        # table_name = args['table_name']
        # options = args['options']
        if args['options'] == "":
            args['options'] = None
        if not w.isconnected():
            w.start()
        ret_data = None
        for nth in range(2):
            ret_data = w.wset(**args)
            error_code = ret_data.ErrorCode
            if error_code != 0:
                if nth == 0 and error_code == -40521010:
                    w.stop()
                    w.start()
                    logger.warning('尝试重新登陆成功，再次调用函数')
                    continue

                msg = ERROR_CODE_MSG_DIC.setdefault(error_code, "")
                logger.error('wset(%s) ErrorCode=%d %s' % (args, error_code, msg))
                raise RequestError(msg, None, error_code)
            else:
                break
        else:
            if ret_data is None:
                msg = 'wst(%s) ret_data is None' % args
                logger.error(msg)
                raise RequestError(msg, None, 0)
        data_count = len(ret_data.Data)
        # if data_count > 0:
        # print('ret_data.Fields\n', ret_data.Fields)
        # ret_data.Data[0] = [format_2_date_str(dt) for dt in ret_data.Data[0]]
        # print('ret_data.Data\n', ret_data.Data)

        for n_data in range(data_count):
            data_list = ret_data.Data[n_data]
            data_list_len = len(data_list)
            if data_list_len > 0:
                # 取出第一个不为None的数据
                item_check = None
                for item_check in data_list:
                    if item_check is not None:
                        break
                # 进行类型检查，如果发现是 datetime, date 类型之一，则进行类型转换
                if item_check is not None and type(item_check) in (datetime, date):
                    ret_data.Data[n_data] = [format_2_date_str(dt) for dt in data_list]
                    logger.debug('Data[%d] column["%s"]  date to str', n_data, ret_data.Fields[n_data])

        ret_df = pd.DataFrame(ret_data.Data, index=ret_data.Fields, columns=ret_data.Codes)
        # print('ret_df\n', ret_df)
        ret_dic = ret_df.to_dict()
        # print('ret_dic:\n', ret_dic)
        return ret_dic


@rec.route('/wsd/')
class ReceiveWSD(Resource):

    @rec.expect(receive_wsd_parser)
    def post(self):
        """
        json str:{"codes": "603555.SH", "fields": "close,pct_chg",
            "begin_time": "2017-01-04", "end_time": "2017-02-28", "options": "PriceAdj=F"}
        :return: 返回万得返回数据dict
        """
        args = receive_wsd_parser.parse_args()
        # print(request.json)
        logger.info('/wsd/ args:%s' % args)
        # codes = args['codes']
        # fields = args['fields']
        # begin_time = args['begin_time']
        # end_time = args['end_time']
        # options = args['options']
        if args['options'] == "":
            args['options'] = None
        if not w.isconnected():
            w.start()
        ret_data = None
        for nth in range(2):
            ret_data = w.wsd(**args)
            error_code = ret_data.ErrorCode
            if error_code != 0:
                if nth == 0 and error_code == -40521010:
                    w.stop()
                    w.start()
                    logger.warning('尝试重新登陆成功，再次调用函数')
                    continue

                msg = ERROR_CODE_MSG_DIC.setdefault(error_code, "")
                logger.error('wsd(%s) ErrorCode=%d %s' % (args, error_code, msg))
                raise RequestError(msg, None, error_code)
            else:
                break
        else:
            if ret_data is None:
                msg = 'wst(%s) ret_data is None' % args
                logger.error(msg)
                raise RequestError(msg, None, 0)
        # 将 Data数据中所有 datetime date 类型的数据转换为 string
        data_len = len(ret_data.Data)

        for n_data in range(data_len):
            data = ret_data.Data[n_data]
            data_len2 = len(data)
            if data_len2 > 0:
                # 取出第一个部位None的数据
                item_check = None
                for item_check in data:
                    if item_check is not None:
                        break
                # 进行类型检查，如果发现是 datetime, date 类型之一，则进行类型转换
                if item_check is not None and type(item_check) in (datetime, date):
                    ret_data.Data[n_data] = [format_2_date_str(dt) for dt in data]
                    logger.info('%d column["%s"]  date to str', n_data, ret_data.Fields[n_data])
        # 组成 DataFrame
        if len(ret_data.Codes) == 1:
            ret_df = pd.DataFrame(ret_data.Data, index=ret_data.Fields,
                                  columns=[format_2_date_str(dt) for dt in ret_data.Times])
        elif len(ret_data.Times) == 1:
            ret_df = pd.DataFrame(ret_data.Data, index=ret_data.Fields,
                                  columns=ret_data.Codes)
        else:
            ret_df = pd.DataFrame(ret_data.Data, index=ret_data.Codes,
                                  columns=[format_2_date_str(dt) for dt in ret_data.Times])
        # print(ret_df)
        ret_dic = ret_df.to_dict()
        # print('ret_dic:\n', ret_dic)
        return ret_dic


@rec.route('/wsi/')
class ReceiveWSI(Resource):

    @rec.expect(receive_wsi_parser)
    def post(self):
        """
        json str:{"codes": "RU1801.SHF", "fields": "open,high,low,close,volume,amt,oi",
            "begin_time": "2017-12-11 09:00:00", "end_time": "2017-12-11 10:27:41", "options": ""}
        :return: 返回万得返回数据dict
        """
        args = receive_wsi_parser.parse_args()
        # print(request.json)
        logger.info('/wsi/ args:%s' % args)
        # codes = args['codes']
        # fields = args['fields']
        # begin_time = args['begin_time']
        # end_time = args['end_time']
        # options = args['options']
        if args['options'] == "":
            args['options'] = None
        if not w.isconnected():
            w.start()
        ret_data = None
        for nth in range(2):
            ret_data = w.wsi(**args)
            error_code = ret_data.ErrorCode
            if error_code != 0:
                if nth == 0 and error_code == -40521010:
                    w.stop()
                    w.start()
                    logger.warning('尝试重新登陆成功，再次调用函数')
                    continue

                msg = ERROR_CODE_MSG_DIC.setdefault(error_code, "")
                logger.error('wsi(%s) ErrorCode=%d %s' % (
                    args, error_code, msg))
                raise RequestError(msg, None, error_code)
            else:
                break
        else:
            if ret_data is None:
                msg = 'wst(%s) ret_data is None' % args
                logger.error(msg)
                raise RequestError(msg, None, 0)
        # 将 Data数据中所有 datetime date 类型的数据转换为 string
        data_len = len(ret_data.Data)

        for n_data in range(data_len):
            data = ret_data.Data[n_data]
            data_len2 = len(data)
            if data_len2 > 0:
                # 取出第一个部位None的数据
                item_check = None
                for item_check in data:
                    if item_check is not None:
                        break
                # 进行类型检查，如果发现是 datetime, date 类型之一，则进行类型转换
                if item_check is not None and type(item_check) in (datetime, date):
                    ret_data.Data[n_data] = [format_2_datetime_str(dt) for dt in data]
                    logger.info('%d column["%s"]  date to str', n_data, ret_data.Fields[n_data])
        # 组成 DataFrame
        ret_df = pd.DataFrame(ret_data.Data, index=ret_data.Fields,
                              columns=[format_2_datetime_str(dt) for dt in ret_data.Times])
        # print(ret_df)
        ret_dic = ret_df.to_dict()
        # print('ret_dic:\n', ret_dic)
        return ret_dic


@rec.route('/wss/')
class ReceiveWSS(Resource):

    @rec.expect(receive_wss_parser)
    def post(self):
        """
        json str:{"codes": "XT1522613.XT",
            "fields": "fund_setupdate,fund_maturitydate,fund_mgrcomp,fund_existingyear,fund_fundmanager", "options": ""}
        :return: 返回万得返回数据dict
        """
        args = receive_wss_parser.parse_args()
        logger.info('/wss/ args:%s', args)
        # codes = args['codes']
        # fields = args['fields']
        # options = args['options']
        if args['options'] == "":
            args['options'] = None
        if not w.isconnected():
            w.start()
        ret_data = None
        for nth in range(2):
            ret_data = w.wss(**args)
            error_code = ret_data.ErrorCode
            if error_code != 0:
                if nth == 0 and error_code == -40521010:
                    w.stop()
                    w.start()
                    logger.warning('尝试重新登陆成功，再次调用函数')
                    continue

                msg = ERROR_CODE_MSG_DIC.setdefault(error_code, "")
                logger.error('wss(%s) ErrorCode=%d %s' % (args, error_code, msg))
                raise RequestError(msg, None, error_code)
            else:
                break
        else:
            if ret_data is None:
                msg = 'wst(%s) ret_data is None' % args
                logger.error(msg)
                raise RequestError(msg, None, 0)
        # 将 Data数据中所有 datetime date 类型的数据转换为 string
        data_len = len(ret_data.Data)
        logger.debug('ret_data.Data len:%d', data_len)
        logger.debug('ret_data.Codes : %s', ret_data.Codes)
        for n_data in range(data_len):
            data = ret_data.Data[n_data]
            data_len2 = len(data)
            if data_len2 > 0:
                if type(data[0]) in (datetime, date):
                    ret_data.Data[n_data] = [format_2_date_str(dt) for dt in data]
                    logger.info('%d column["%s"]  date to str', n_data, ret_data.Fields[n_data])
        # print('ret_data.Data:\n', ret_data.Data)
        # 组成 DataFrame
        ret_df = pd.DataFrame(ret_data.Data, index=ret_data.Fields, columns=ret_data.Codes)
        ret_dic = ret_df.to_dict()
        # print('ret_dic:\n', ret_dic)
        return ret_dic


@rec.route('/tdaysoffset/')
class ReceiveTdaysoffset(Resource):

    @rec.expect(tdays_offset_parser)
    def post(self):
        """
        json str:{"offset": "1", "begin_time": "2017-3-31", "options": ""}
        :return: 返回万得返回数据dict
        """
        args = tdays_offset_parser.parse_args()
        logger.info('/tdaysoffset/ args:%s', args)
        # offset = int(args['offset'])
        # begin_time = args['begin_time']
        # options = args['options']
        if args['options'] == "":
            args['options'] = None
        if not w.isconnected():
            w.start()
        ret_data = None
        for nth in range(2):
            ret_data = w.tdaysoffset(**args)
            error_code = ret_data.ErrorCode
            if error_code != 0:
                if nth == 0 and error_code == -40521010:
                    w.stop()
                    w.start()
                    logger.warning('尝试重新登陆成功，再次调用函数')
                    continue

                msg = ERROR_CODE_MSG_DIC.setdefault(error_code, "")
                logger.error(
                    'tdaysoffset("%s") ErrorCode=%d %s' % (args, error_code, msg))
                raise RequestError(msg, None, error_code)
            else:
                break
        else:
            if ret_data is None:
                msg = 'wst(%s) ret_data is None' % args
                logger.error(msg)
                raise RequestError(msg, None, 0)
        # 将 Data数据中所有 datetime date 类型的数据转换为 string
        if len(ret_data.Data) > 0 and len(ret_data.Data[0]) > 0:
            date_str = format_2_date_str(ret_data.Data[0][0])
        else:
            logger.warning('tdaysoffset(%s) No value return' % args)
            date_str = ''
        ret_dic = {'Date': date_str}
        # print('offset:\n', ret_dic)
        return ret_dic


@rec.route('/tdays/')
class ReceiveTdays(Resource):

    @rec.expect(tdays_parser)
    def post(self):
        """
        json str:{"begin_time": "2017-3-31", "end_time": "2017-3-31", "options": ""}
        :return: 返回万得返回数据dict
        """
        args = tdays_parser.parse_args()
        logger.info('/tdays/ args:%s', args)
        # begin_time = args['begin_time']
        # end_time = args['end_time']
        # options = args['options']
        if args['options'] == "":
            args['options'] = None
        if not w.isconnected():
            w.start()
        ret_data = None
        for nth in range(2):
            ret_data = w.tdays(**args)
            error_code = ret_data.ErrorCode
            if error_code != 0:
                if nth == 0 and error_code == -40521010:
                    w.stop()
                    w.start()
                    logger.warning('尝试重新登陆成功，再次调用函数')
                    continue

                msg = ERROR_CODE_MSG_DIC.setdefault(error_code, "")
                logger.error('tdays(%s) ErrorCode=%d %s' % (args, error_code, msg))
                raise RequestError(msg, None, error_code)
            else:
                break
        else:
            if ret_data is None:
                msg = 'wst(%s) ret_data is None' % args
                logger.error(msg)
                raise RequestError(msg, None, 0)
        # 将 Data数据中所有 datetime date 类型的数据转换为 string
        if len(ret_data.Data) > 0 and len(ret_data.Data[0]) > 0:
            # date_str = format_datetime_to_str(ret_data.Data[0][0])
            # ret_df = pd.DataFrame({'date': [format_datetime_to_str(d) for d in ret_data.Data[0]]})
            # ret_df.index = [str(idx) for idx in ret_df.index]
            # ret_dic = {'date': [format_datetime_to_str(d) for d in ret_data.Data[0]]}
            ret_dic = [format_2_date_str(d) for d in ret_data.Data[0]]
        else:
            logger.warning('tdays(%s) No value return' % args)
            ret_dic = []
        # ret_dic = ret_df.to_dict()
        # print('tdays:\n', ret_dic)
        return ret_dic


@rec.route('/wsq/')
class ReceiveWSQ(Resource):

    @rec.expect(receive_wsq_parser)
    def post(self):
        """
        json str:{"codes": "600008.SH,600010.SH,600017.SH", "fields": "rt_open,rt_low_limit", "options": ""}
        :return: 返回万得返回数据dict
        """
        args = receive_wsq_parser.parse_args()
        logger.info('/wsq/ args:%s', args)
        # codes = args['codes']
        # fields = args['fields']
        # options = args['options']
        if args['options'] == "":
            args['options'] = None
        if not w.isconnected():
            w.start()
        ret_data = None
        for nth in range(2):
            ret_data = w.wsq(**args)
            error_code = ret_data.ErrorCode
            if error_code != 0:
                if nth == 0 and error_code == -40521010:
                    w.stop()
                    w.start()
                    logger.warning('尝试重新登陆成功，再次调用函数')
                    continue

                msg = ERROR_CODE_MSG_DIC.setdefault(error_code, "")
                logger.error('wsq(%s) ErrorCode=%d %s' % args)
                raise RequestError(msg, None, error_code)
            else:
                break
        else:
            if ret_data is None:
                msg = 'wst(%s) ret_data is None' % args
                logger.error(msg)
                raise RequestError(msg, None, 0)
        # 将 Data数据中所有 datetime date 类型的数据转换为 string
        data_len = len(ret_data.Data)
        logger.debug('ret_data.Data len:%d', data_len)
        logger.debug('ret_data.Codes : %s', ret_data.Codes)
        for n_data in range(data_len):
            data = ret_data.Data[n_data]
            data_len2 = len(data)
            if data_len2 > 0:
                if type(data[0]) in (datetime, date):
                    ret_data.Data[n_data] = [format_2_date_str(dt) for dt in data]
                    logger.info('%d column["%s"]  date to str', n_data, ret_data.Fields[n_data])
        # print('ret_data.Data:\n', ret_data.Data)
        # 组成 DataFrame
        ret_df = pd.DataFrame(ret_data.Data, index=ret_data.Fields, columns=ret_data.Codes)
        ret_dic = ret_df.to_dict()
        # print('ret_dic:\n', ret_dic)
        return ret_dic


@rec.route('/wst/')
class ReceiveWST(Resource):

    @rec.expect(receive_wst_parser)
    def post(self):
        """
        json str:{"codes": "600008.SH, "fields": "ask1,bid1,asize1,bsize1,volume,amt,pre_close,open,high,low,last",
            "begin_time": "2017-01-04", "end_time": "2017-02-28", "options": ""}
        :return: 返回万得返回数据dict
        """
        args = receive_wst_parser.parse_args()
        logger.info('/wst/ args:%s', args)
        # codes = args['codes']
        # fields = args['fields']
        # begin_time = args['begin_time']
        # end_time = args['end_time']
        # options = args['options']
        if args['options'] == "":
            args['options'] = None
        if not w.isconnected():
            w.start()
        ret_data = None
        for nth in range(2):
            ret_data = w.wst(**args)
            error_code = ret_data.ErrorCode
            if error_code != 0:
                if nth == 0 and error_code == -40521010:
                    w.stop()
                    w.start()
                    logger.warning('尝试重新登陆成功，再次调用函数')
                    continue

                msg = ERROR_CODE_MSG_DIC.setdefault(error_code, "")
                logger.error('wst(%s) ErrorCode=%d %s' % (args, error_code, msg))
                raise RequestError(msg, None, error_code)
            else:
                break
        else:
            if ret_data is None:
                msg = 'wst(%s) ret_data is None' % args
                logger.error(msg)
                raise RequestError(msg, None, 0)
        # 将 Data数据中所有 datetime date 类型的数据转换为 string
        data_len = len(ret_data.Data)

        for n_data in range(data_len):
            data = ret_data.Data[n_data]
            data_len2 = len(data)
            if data_len2 > 0:
                # 取出第一个部位None的数据
                item_check = None
                for item_check in data:
                    if item_check is not None:
                        break
                # 进行类型检查，如果发现是 datetime, date 类型之一，则进行类型转换
                if item_check is not None and type(item_check) in (datetime, date):
                    ret_data.Data[n_data] = [format_2_datetime_str(dt) for dt in data]
                    logger.info('%d column["%s"]  date to str', n_data, ret_data.Fields[n_data])
        # 组成 DataFrame
        ret_df = pd.DataFrame(ret_data.Data, index=ret_data.Fields,
                              columns=[format_2_datetime_str(dt) for dt in ret_data.Times])
        # print(ret_df)
        ret_dic = ret_df.to_dict()
        # print('ret_dic:\n', ret_dic)
        return ret_dic


@rec.route('/edb/')
class ReceiveEDB(Resource):

    @rec.expect(receive_edb_parser)
    def post(self):
        """
        json str:{"codes": "M0017126,M0017127,M0017128",
            "begin_time": "2016-11-10", "end_time": "2017-11-10", "options": "Fill=Previous"}
        :return: 返回万得返回数据dict
        """
        args = receive_edb_parser.parse_args()
        logger.info('/edb/ args:%s', args)
        codes = args['codes']
        # begin_time = args['begin_time']
        # end_time = args['end_time']
        # options = args['options']
        if args['options'] == "":
            args['options'] = None
        if not w.isconnected():
            w.start()
        ret_data = None
        for nth in range(2):
            ret_data = w.edb(**args)
            error_code = ret_data.ErrorCode
            if error_code != 0:
                if nth == 0 and error_code == -40521010:
                    w.stop()
                    w.start()
                    logger.warning('尝试重新登陆成功，再次调用函数')
                    continue

                msg = ERROR_CODE_MSG_DIC.setdefault(error_code, "")
                logger.error('wst(%s) ErrorCode=%d %s' % (args, error_code, msg))
                raise RequestError(msg, None, error_code)
            else:
                break
        else:
            if ret_data is None:
                msg = 'wst(%s) ret_data is None' % args
                logger.error(msg)
                raise RequestError(msg, None, 0)

        # 将 Data数据中所有 datetime date 类型的数据转换为 string
        data_len = len(ret_data.Data)

        for n_data in range(data_len):
            data = ret_data.Data[n_data]
            data_len2 = len(data)
            if data_len2 > 0:
                # 取出第一个部位None的数据
                item_check = None
                for item_check in data:
                    if item_check is not None:
                        break
                # 进行类型检查，如果发现是 datetime, date 类型之一，则进行类型转换
                if item_check is not None and type(item_check) in (datetime, date):
                    ret_data.Data[n_data] = [format_2_date_str(dt) for dt in data]
                    logger.info('%d column["%s"]  date to str', n_data, ret_data.Fields[n_data])
        # 组成 DataFrame
        ret_df = pd.DataFrame(ret_data.Data, index=[xx.strip() for xx in codes.split(',')],
                              columns=[format_2_date_str(dt) for dt in ret_data.Times])
        # print(ret_df)
        ret_dic = ret_df.to_dict()
        # print('ret_dic:\n', ret_dic)
        return ret_dic
