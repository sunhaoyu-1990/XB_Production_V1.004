# coding=gbk
import time
import datetime
import warnings
import numpy as np
from fastapi import FastAPI, Form
from fastapi.responses import JSONResponse

import pandas as pd

import Parameter_Comput as pc
import Mysql_Operation as mop
import Data_analysis_2 as da
import Monte_Carlo_method as mcm
import Data_Basic_Function as dbf
import Keyword_and_Parameter as kp
import Interface
from apscheduler.schedulers.blocking import BlockingScheduler

warnings.filterwarnings("ignore")
app = FastAPI()

# 创建全局变量
start_num = 0
compute_num = 0
gantrys = []
stations = []
gantry_back_data = {}
last_station_dict = {}
start_stake_dict = {}
end_stake_dict = {}
speed_threshold_data = {}
old_inout_data = {}
flow_history = {}
new_inout_data = {}
new_flow_history = {}
province_gantry = []


# 基础参数获取类
class parameter:
    Gantrys = mop.load_data_from_mysql('overspeed_mysql', 'road_divide_flow_config', ['ROAD_ID'], ['G0030610030'],
                                       ['='], get_feature=['TOLL_INTERVALS']).values
    Gantrys = Gantrys[0][0].split('|')

    Stations = mop.load_data_from_mysql('overspeed_mysql', 'road_divide_flow_config', ['ROAD_ID'], ['G0030610030'],
                                        ['='], get_feature=['TOLL_INTERVALS']).values
    Stations = Stations[0][0].split('|')
    # 获取收费单元和上一个单元的对应关系字典
    gantry_back_data = dbf.get_disc_from_document('./Data_Origin/tom_noderelation.csv',
                                                  ['EXROADNODEID', 'ENROADNODEID'],
                                                  encoding='gbk', key_for_N=True, key_for_N_type='list')

    # 获取收费单元和上一个收费站的对应关系字典
    last_station_dict = dbf.get_disc_from_document('./Data_Origin/tollinterval.csv',
                                                   ['id', 'enTollStation'], encoding='utf-8', key_for_N=False)

    # 获取收费单元start stake字典
    start_stake_dict = dbf.get_disc_from_document('./Data_Origin/tollinterval.csv', ['id', 'startStakeNum'],
                                                  encoding='utf-8', key_for_N=False, key_for_N_type='list')

    # 获取收费单元end stake字典
    end_stake_dict = dbf.get_disc_from_document('./Data_Origin/tollinterval.csv', ['id', 'endStakeNum'],
                                                encoding='utf-8', key_for_N=False, key_for_N_type='list')

    # 获取各路段的速度阈值
    speed_threshold_data = dbf.get_disc_from_document('./statistic_data/basic_data/gantry_speed_threshold.csv',
                                                      ['id', 'speed'], encoding='utf-8', key_for_N=False)

    # 获取入省省界门架ID集
    province_gantry = da.get_data_some_col_have_save_value(['./Data_Origin/tollinterval.csv'], ['provinceType'], ['2'],
                                                           ['='], ifIndex=False, ifSave=False, save_columns=['id'])


'''
    创建时间：2022/8/26
    完成时间：2022/9/29
    功能：实际拥堵长度计算函数
    修改时间：
'''


@app.post("/compute_congestion_length")
async def compute_congestion_length(INTERVAL_ID: str = Form(None, max_length=16, min_length=16),
                                    EVENT_STAKE_NUM: float = Form(None),
                                    TIME_POINT: str = Form(None, max_length=19, min_length=19)):
    """
    实际拥堵长度计算函数
    :param TIME_POINT:
    :param EVENT_STAKE_NUM: 事件地点位置的桩号
    :param INTERVAL_ID: 收费单元ID
    :return:
    """
    # 根据提供的地点，进行拥堵点上下游的比例计算
    # 获取各收费单元的起止桩号信息
    interval_data = dbf.get_disc_from_document('./Data_Origin/tollinterval.csv',
                                               ['id', 'startStakeNum', 'endStakeNum'],
                                               encoding='utf-8', key_for_N=False)
    # 计算上游部分的比例
    rate = abs(EVENT_STAKE_NUM - float(interval_data[0])) / abs(float(interval_data[1]) - float(interval_data[0]))
    # 进行加权处理
    rate = np.sin(rate * 2 / np.pi)
    rate = [rate, 1 - rate]

    # 获取每个车型的当前承载量
    now_have_num = mop.load_data_from_mysql('overspeed_mysql', 'interval_flow', ['INTERVAL_ID', 'TIME_POINT'],
                                            [INTERVAL_ID, TIME_POINT], ['=', '='])
    # 将承载量转换为字典类型
    now_have_num = dbf.get_disc_from_document(now_have_num.values, [4, 7], encoding='utf-8',
                                              input_type='list')

    # 进行模拟计算车辆长度
    LENGTH = mcm.monte_carlo_main(now_have_num, rate)

    # 承载量写入数据库
    now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    upload_data = []
    data_ls = [INTERVAL_ID, 'G0030610030', TIME_POINT, EVENT_STAKE_NUM, now_time, 0, LENGTH]
    upload_data.append(data_ls)
    mop.write_data_to_mysql(upload_data, 'overspeed_mysql', 'interval_congestion_detail_name',
                            'interval_congestion_detail_features', 'interval_congestion_detail_features_type',
                            'csv')

    return JSONResponse(
        content={'INTERVAL_ID': INTERVAL_ID, 'ROAD_ID': 'G0030610030', 'EVENT_STAKE_NUM': EVENT_STAKE_NUM,
                 'TIME_POINT': TIME_POINT, 'LENGTH': LENGTH}, status_code=202, headers={'type': 'congestion_length'})


'''
    创建时间：2022/10/19
    完成时间：2022/10/29
    功能：定时确定基础数据上传时间，如果有更新，就进行拥堵感知运算
    修改时间：No.1 2022/10/31, update the charge if now_flow_data have 280 things
            No.2 2023/01/28，增加收费站基础表的数据、门架ID集和收费站ID集的获取，修改数据完整情况判断条件
'''


@app.get("./start_function_by_period/")
def start_function_by_period():
    """
    计算各路段各时段的输入输出车流量
    :return:
    """
    print('begin the update time check-------------------', time.strftime('%Y-%m-%d %H:%M:%S',
                                                                          time.localtime(time.time())))
    # 获取流入流出表的最新更新时间
    upload_inout_time = mop.load_data_from_mysql('overspeed_mysql', 'interval_flow_upload_time', [], [], [])

    # 计算下一个时间点
    next_time = datetime.datetime.strftime(
        datetime.datetime.strptime(upload_inout_time.iloc[0, 0], '%Y-%m-%d %H:%M:%S') +
        datetime.timedelta(minutes=5), '%Y-%m-%d %H:%M:%S')
    print(next_time)

    #
    # 获取下一个时间点收费单元基础表的数据
    print("get next time base data")
    now_flow_data = mop.load_data_from_mysql('overspeed_mysql', 'interval_flow_base', ['TIME_POINT'], [next_time],
                                             ['='])  # 2022/10/31
    # 获取下一个时间点收费站基础表的数据，changed by 2023/01/28
    now_station_flow_data = mop.load_data_from_mysql('overspeed_mysql', 'station_flow', ['TIME_POINT'], [next_time],
                                                     ['='])
    # 判断是否为首次运行，首次运行加载所有的基础参数
    global gantrys, stations, start_num, gantry_back_data, last_station_dict, start_stake_dict, \
        end_stake_dict, speed_threshold_data, province_gantry
    if start_num == 0:
        # gantrys = vars(parameter)['Gantrys']
        gantrys = ['G000561001000110', 'G000561001000210']
        # stations = vars(parameter)['Stations']
        stations = ['G0005610010010', 'G0005610010110']
        gantry_back_data = vars(parameter)['gantry_back_data']
        last_station_dict = vars(parameter)['last_station_dict']
        start_stake_dict = vars(parameter)['start_stake_dict']
        end_stake_dict = vars(parameter)['end_stake_dict']
        speed_threshold_data = vars(parameter)['speed_threshold_data']
        province_gantry = vars(parameter)['province_gantry']
        start_num += 1

    now_station_flow_data_ls = now_station_flow_data.drop_duplicates(subset=['STATION_ID'])
    # 判断获取到的数据是否为空
    if now_flow_data.shape[0] != len(gantrys) or now_station_flow_data_ls.shape[0] != len(
            stations):  # 2023/01/28 update
        # 如果没有更新，结束，等待下一次开启
        print('the data is the old, continue wait')
        pass
    else:
        # 如果基础表数据有更新，开启运算
        print('have new data, begin compute')
        compute_level_of_congestion(next_time, now_station_flow_data, now_flow_data)
    print('end the update time check-------------------', time.strftime('%Y-%m-%d %H:%M:%S',
                                                                        time.localtime(time.time())))


'''
    创建时间：2022/8/26
    完成时间：2022/10/14
    功能：各路段各时段的拥堵等级和拥堵预警计算
    修改时间：No.1 2023/01/28,增加收费站当前时刻数据的输入，修改路段流入流出和承载量的输入参数
'''


def compute_level_of_congestion(time_point, now_station_flow_data, now_flow_data):
    """
    计算各路段各时段的输入输出车流量
    :param now_station_flow_data:
    :param DataFrame now_flow_data: 当前时刻的基础信息（包括各门架流量和速度）
    :param str time_point: 进行计算的时间段值，格式“YYYY-MM-DD hh:mm:ss"
    :return:
    """
    global gantrys, stations, compute_num, old_inout_data, flow_history, new_inout_data, new_flow_history, \
        province_gantry
    print('get the basic data-----------')
    congestion_upload = 0

    # 获取各收费单元上一时段的承载量
    print("get last have num from mysql:start-----------")
    last_time = datetime.datetime.strftime(datetime.datetime.strptime(time_point, '%Y-%m-%d %H:%M:%S') +
                                           datetime.timedelta(minutes=-5), '%Y-%m-%d %H:%M:%S')
    print("last time is " + last_time)
    # 2022/10/31 update

    if last_time[-8:] == '00:00:00':
        last_have_dict = {}
        gantrys = vars(parameter)['Gantrys']
        stations = vars(parameter)['Stations']
        global gantry_back_data, last_station_dict
        gantry_back_data = vars(parameter)['gantry_back_data']
        last_station_dict = vars(parameter)['last_station_dict']
        province_gantry = vars(parameter)['province_gantry']

    else:
        last_have_num = mop.load_data_from_mysql('overspeed_mysql', 'interval_flow', ['TIME_POINT'], [last_time], ['='])

        last_have_dict = dbf.get_disc_from_document(last_have_num.values, [1, 4, 7], ifIndex=False,
                                                    encoding='utf-8', key_length=2, sign='_', input_type='list')
    print("get last have num from mysql:end-----------last_have_num:" + str(len(last_have_dict)))

    # 将速度数据转换为字典类型
    now_speed_dict = dbf.get_disc_from_document(now_flow_data.values, [0, 1, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25],
                                                ifIndex=False, encoding='utf-8', key_length=2, sign='_',
                                                input_type='list')

    # 将流量数据转换为字典类型
    now_flow_vType_dict = dbf.get_disc_from_document(now_flow_data.values, [0, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                                                     ifIndex=False, encoding='utf-8', key_length=1, sign='_',
                                                     input_type='list')

    # 将收费站流量数据转换为字典类型
    now_station_flow_vType_dict = dbf.get_disc_from_document(now_station_flow_data.values,
                                                             [0, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                                                             ifIndex=False, encoding='utf-8', key_length=3, sign='_',
                                                             input_type='list')
    # 计算各收费单元路段当前时间的输入输出流量和承载量, change:2023/01/28
    print("compute the inout num and have num:-----------start")
    gantry_have_data, gantry_inout_data, gantry_have_total_data, gantry_inout_total_data = \
        pc.compute_num_of_gantry(now_flow_vType_dict, now_station_flow_vType_dict, last_have_dict, gantry_back_data,
                                 last_station_dict, time_point, province_gantry, gantrys=gantrys)
    print("compute the inout num and have num:-----------end")

    if compute_num == 0:
        # 获取各收费单元路段当前时间往前13个时段的流入流出数据
        print("get 13 inout num of gantrys before this time from mysql:-----------start")
        last_time_list = [datetime.datetime.strftime(datetime.datetime.strptime(time_point, '%Y-%m-%d %H:%M:%S') +
                                                     datetime.timedelta(minutes=(5 * (-1 * (j + 1)))), '%Y-%m-%d '
                                                                                                       '%H:%M:%S')
                          for j in range(13)]
        print("inout num last_time_list data num:" + str(len(last_time_list)))
        old_inout_data = mop.load_data_from_mysql('overspeed_mysql', 'interval_flow', ['TIME_POINT'], [last_time_list],
                                                  ['in'])
        print("old_inout_data data total num:" + str(old_inout_data.shape[0]))
        # print("old_inout_data data num:" + str(round(old_inout_data.shape[0]/(len(gantrys) * 10),1)))
        print("get 13 inout num of gantrys before this time from mysql:-----------end")

        # 获取各收费单元路段当前时间往前48个时段的流量数据
        print("get 48 flow of gantrys before this time from mysql:-----------start")
        last_time_list = [datetime.datetime.strftime(datetime.datetime.strptime(time_point, '%Y-%m-%d %H:%M:%S') +
                                                     datetime.timedelta(minutes=(5 * (-1 * (j + 1)))),
                                                     '%Y-%m-%d %H:%M:%S') for j in range(48)]
        print("flow data last_time_list data num:" + str(len(last_time_list)))
        flow_history = mop.load_data_from_mysql('overspeed_mysql', 'interval_flow_base', ['TIME_POINT'],
                                                [last_time_list], ['in'])
        print("flow_history data num:" + str(round(flow_history.shape[0] / (len(gantrys) * 10), 1)))
        print("get 48 flow of gantrys before this time from mysql:-----------end")
        # 进行13个时段流入流出数据的转换
        if old_inout_data.empty:
            pass
        else:
            old_inout_data = old_inout_data[['INTERVAL_ID', 'TIME_POINT', 'IN_TOTAL', 'OUT_TOTAL']]
            old_inout_data = old_inout_data.sort_values(['TIME_POINT'])
        # 进行48个时段流量数据的转换
        flow_history = flow_history.sort_values(['TIME_POINT'])
        flow_history['TIME_POINT'] = flow_history['TIME_POINT'].astype(str)
        flow_history['gantry_time'] = flow_history['INTERVAL_ID'] + '_' + flow_history['TIME_POINT']
        columns = ['gantry_time', "FLOW_1", "FLOW_2", "FLOW_3", "FLOW_4", "FLOW_11", "FLOW_12", "FLOW_13",
                   "FLOW_14", "FLOW_15", "FLOW_16"]
        flow_history = flow_history[columns]
        flow_history = flow_history.set_index('gantry_time', drop=True).unstack()
        flow_history = flow_history.reset_index()
        flow_history.columns = ['VEHICLE_TYPE', 'gantry_time', 'FLOW']
        flow_history['INTERVAL_ID'] = flow_history['gantry_time'].map(lambda x: x.split('_')[0])
        flow_history = flow_history[['VEHICLE_TYPE', 'INTERVAL_ID', 'FLOW']]
        compute_num += 1

    else:
        # 获取各收费单元路段当前时间往前13个时段的流入流出数据
        print("update 13 inout num of gantrys before this time from mysql:-----------start")
        old_inout_data = pd.concat((old_inout_data, new_inout_data))
        print("old_inout_data data total num:" + str(old_inout_data.shape[0]))
        # print("old_inout_data data num:" + str(round(old_inout_data.shape[0]/(len(gantrys) * 10),1)))
        print("update 13 inout num of gantrys before this time from mysql:-----------end")

        # 获取各收费单元路段当前时间往前48个时段的流量数据
        print("update 48 flow of gantrys before this time from mysql:-----------start")
        flow_history = pd.concat((flow_history, new_flow_history))
        print("flow_history data num:" + str(round(flow_history.shape[0] / (len(gantrys) * 10), 1)))
        print("update 48 flow of gantrys before this time from mysql:-----------end")

    level_result = {}
    # gantry_inout_data_new = dbf.compute_dict_by_group(gantry_inout_data, [0, 2], 'sum', '_')
    gantry_inout_data_new = gantry_inout_total_data
    # gantry_have_data_new = dbf.compute_dict_by_group(gantry_have_data, [0], 'sum', '_')
    gantry_have_data_new = gantry_have_total_data  # changed at 2023/01/29

    # 开始进行拥堵等级判定
    if old_inout_data.empty:  # 如果没有输入输出的历史数据，直接跳过
        old_inout_data = pd.DataFrame(data=None, columns=['INTERVAL_ID', 'TIME_POINT', 'IN_TOTAL', 'OUT_TOTAL'])
        pass
    else:
        flow_history_dict = flow_history.groupby(['INTERVAL_ID', 'VEHICLE_TYPE'])['FLOW'].apply(
            lambda x: list(x)).reset_index()
        old_inout_data_ls = old_inout_data.groupby(['INTERVAL_ID'])['IN_TOTAL'].count().reset_index()
        # 如果历史数据满足数据量要求，即进行拥堵等级判定
        if len(flow_history_dict.iloc[0, 2]) < 48 or old_inout_data_ls.iloc[0, 1] < 13:
            pass
        else:
            print('begin congestion level compute-------------------------------------------------')
            flow_history_dict = dbf.get_disc_from_document(flow_history_dict.values, [0, 1, 2], key_length=2, sign='_',
                                                           ifIndex=False, key_for_N=False, encoding='utf-8',
                                                           input_type='list')
            congestion_upload = 1
            # 将流入流出数据转换为字典
            old_inout_data_in = dbf.get_disc_from_document(old_inout_data.values, [0, 2], ifIndex=False, key_for_N=True,
                                                           key_for_N_type='list', encoding='utf-8',
                                                           input_type='list')
            old_inout_data_out = dbf.get_disc_from_document(old_inout_data.values, [0, 3], encoding='utf-8',
                                                            key_for_N=True, key_for_N_type='list', ifIndex=False,
                                                            input_type='list')

            # 通过速度情况进行拥堵判定
            print('congestion level compute by speed:-----------start')
            congestion_charge_speed = pc.charge_congestion_with_speed(now_speed_dict, time_point, now_flow_vType_dict,
                                                                      speed_threshold_data, gantrys=gantrys)
            print("result count num:" + str(len(congestion_charge_speed)))
            print('congestion level compute by speed:-----------end')

            # 进行各路段输入输出流量曲线相似度计算，判断该路段是否已发生拥堵
            print('congestion level compute by flow:-----------start')
            congestion_charge_flow = pc.charge_congestion_with_flow_curve(old_inout_data_in, old_inout_data_out,
                                                                          gantry_inout_data_new, gantry_back_data,
                                                                          gantrys=gantrys)
            print("result count num:" + str(len(congestion_charge_flow)))
            print('congestion level compute by flow:-----------end')

            # 计算目前各路段的车辆数量及半小时后数量，并与各路段承载阈值量进行对比
            print('congestion level compute by have num:-----------start')
            congestion_charge_have = pc.charge_congestion_by_haveNum(gantry_have_data_new, time_point,
                                                                     flow_history_dict, gantry_back_data,
                                                                     last_station_dict, province_gantry,
                                                                     gantrys=gantrys)
            print("result count num:" + str(len(congestion_charge_have)))
            print('congestion level compute by have num:-----------end')

            # 根据目前各路段的车辆数量进行拥堵等级判定
            print('congestion level result put out:-----------start')
            level_result = pc.charge_congestion_level_by_result(congestion_charge_speed, congestion_charge_flow,
                                                                congestion_charge_have, gantry_have_data, time_point)
            print("result count num:" + str(len(level_result)))
            print('congestion level result put out:-----------end')
            print('end congestion level compute----------------------------------------------------')
            # 删掉输入输出历史数据中的最早数据
            last_time_list = datetime.datetime.strftime(datetime.datetime.strptime(time_point, '%Y-%m-%d %H:%M:%S') +
                                                        datetime.timedelta(minutes=(-65)), '%Y-%m-%d %H:%M:%S')
            old_inout_data = old_inout_data[old_inout_data['TIME_POINT'] != last_time_list]

            # 删掉最早的流量历史数据
            flow_history = flow_history.iloc[len(new_flow_history), :]

    # 计算新时刻的各车型流量数据
    now_flow_data['TIME_POINT'] = now_flow_data['TIME_POINT'].astype(str)
    now_flow_data['gantry_time'] = now_flow_data['INTERVAL_ID'] + '_' + now_flow_data['TIME_POINT']
    columns = ['gantry_time', "FLOW_1", "FLOW_2", "FLOW_3", "FLOW_4", "FLOW_11", "FLOW_12", "FLOW_13",
               "FLOW_14", "FLOW_15", "FLOW_16"]
    now_flow_data = now_flow_data[columns]
    now_flow_data = now_flow_data.set_index('gantry_time', drop=True).unstack()
    new_flow_history = now_flow_data.reset_index()
    new_flow_history.columns = ['VEHICLE_TYPE', 'gantry_time', 'FLOW']
    new_flow_history['INTERVAL_ID'] = new_flow_history['gantry_time'].map(lambda x: x.split('_')[0])
    new_flow_history = new_flow_history[['VEHICLE_TYPE', 'INTERVAL_ID', 'FLOW']]

    # 进行结果的数据库上传
    # if the upload time equal this time
    print('begin mysql database upload------------------------------------------------------')
    upload_time = mop.load_data_from_mysql('overspeed_mysql', 'interval_flow_upload_time', [], [], [])
    last_time = datetime.datetime.strftime(datetime.datetime.strptime(time_point, '%Y-%m-%d %H:%M:%S') +
                                           datetime.timedelta(minutes=(-5)), '%Y-%m-%d %H:%M:%S')
    if upload_time.empty or last_time == upload_time.iloc[0, 0]:
        # 承载量写入数据库
        print('write inout num to mysql:--------------------------start')
        upload_data = []
        time_num = str((float(time_point[11:13]) * 60 + float(time_point[14:16])) / 5 + 1)
        for key in gantry_have_data.keys():
            key_list = key.split('_')
            data_ls = [key_list[0], time_point, time_num]
            data_ls.extend(gantry_inout_data[key_list[0] + '_in'])
            data_ls.append(gantry_inout_total_data[key_list[0] + '_in'])
            data_ls.extend(gantry_inout_data[key_list[0] + '_out'])
            data_ls.append(gantry_inout_total_data[key_list[0] + '_out'])
            data_ls.extend(gantry_have_data[key])
            data_ls.append(gantry_have_total_data[key_list[0]])
            upload_data.append(data_ls)
        mop.write_data_to_mysql(upload_data, 'overspeed_mysql', 'interval_flow_name', 'interval_flow_features',
                                'interval_flow_features_type', 'csv')
        # 计算新时刻的各路段输入输出数据
        columns = kp.get_parameter_with_keyword('interval_flow_features')
        new_inout_data = pd.DataFrame(upload_data, columns=columns)
        new_inout_data = new_inout_data[["INTERVAL_ID", "TIME_POINT", "IN_TOTAL", "OUT_TOTAL"]]
        print('write inout num to mysql:----------------------end')

        if upload_time.empty:
            print('write inout num upload_time to mysql:----------------------start')
            mop.write_data_to_mysql([[time_point]], 'overspeed_mysql', 'interval_flow_upload_name',
                                    'interval_flow_upload_features',
                                    'interval_flow_upload_features_type', 'csv')
            print('write inout num upload_time to mysql:----------------------end')
        else:
            print('update inout num upload_time to mysql:----------------------start')
            mop.update_data_of_mysql('overspeed_mysql', 'interval_flow_upload_time', ['UPLOAD_TIME'], [[time_point]],
                                     [[]], [[]], [[]])
            print('update inout num upload_time to mysql:----------------------end')

    else:
        print('this time is ' + time_point + ' and interval_flow upload time is ' + upload_time.iloc[0, 0])

    if congestion_upload == 1:
        # if the upload time equal this time
        upload_time = mop.load_data_from_mysql('overspeed_mysql', 'interval_congestion_upload_time', [], [], [])
        if upload_time.empty or last_time == upload_time.iloc[0, 0]:
            # 拥堵等级结果上传
            print('write congestion level data to mysql:------------start')
            for j in range(len(level_result)):
                list_ls = ['G0030610030', level_result[j][0]]
                try:
                    have_num = gantry_have_data_new[level_result[j][0]]
                except Exception:
                    have_num = 0
                list_ls.extend([time_point, start_stake_dict[level_result[j][0]],
                                end_stake_dict[level_result[j][0]], level_result[j][1], level_result[j][2],
                                have_num, level_result[j][3], 0])
                level_result[j] = list_ls
            mop.write_data_to_mysql(level_result, 'overspeed_mysql', 'interval_congestion_name',
                                    'interval_congestion_features', 'interval_congestion_features_type', 'csv')
            print('write congestion level data to mysql:------------end')

            if upload_time.empty:
                print('write congestion level upload_time data to mysql:------------start')
                mop.write_data_to_mysql([[time_point]], 'overspeed_mysql', 'interval_congestion_upload_name',
                                        'interval_congestion_upload_features',
                                        'interval_congestion_upload_features_type', 'csv')
                print('write congestion level upload_time data to mysql:------------end')
            else:
                print('update congestion level upload_time data to mysql:------------start')
                mop.update_data_of_mysql('overspeed_mysql', 'interval_congestion_upload_time', ['UPLOAD_TIME'],
                                         [[time_point]], [[]], [[]], [[]])
                print('write congestion level upload_time data to mysql:------------end')

        else:
            print('this time is ' + time_point + ' and interval_congestion upload time is ' + upload_time.iloc[0, 0])
    print('post requests to gaoGuanTong:------------start')  # 2022/11/15 add
    interface = Interface.interface_manager(target='ETC_enable', data={"roadId": "G0030610030",
                                                                       "timePoint": time_point})  # 2022/11/11 add
    response = interface.request_function()  # 2022/11/11 add
    print('response result is:', response)  # 2022/11/15 add
    print('post requests to gaoGuanTong:------------end')  # 2022/11/15 add

    print('end mysql database upload------------------------------------------------------')


'''
    创建时间：2022/10/19
    完成时间：2022/10/19
    功能：主函数
    修改时间：
'''


def main(period_time):
    """
    主函数
    :param int period_time: 定时循环的时间间隔，单位秒
    :return:
    """
    scheduler = BlockingScheduler()
    scheduler.add_job(start_function_by_period, 'interval', seconds=period_time)
    scheduler.start()


if __name__ == '__main__':
    # last_time_list = [
    #     datetime.datetime.strftime(datetime.datetime.strptime('2022-07-01 00:00:00', '%Y-%m-%d %H:%M:%S') +
    #                                datetime.timedelta(minutes=(5 * (1 * i))),
    #                                '%Y-%m-%d %H:%M:%S') for i in range(20600)]
    # for t in last_time_list:
    #     compute_level_of_congestion(t)
    #     compute_level_of_congestion_by_history(t)
    # for i in range(3):
    #     start_function_by_period()
    # main(1)
    # data = pd.read_csv('./Data_Origin/test.csv')
    # data_station = pd.read_csv('./Data_Origin/test_station.csv')
    # data = data.fillna('')
    # data_station = data_station.fillna('')
    # last_time_list = [
    #     datetime.datetime.strftime(datetime.datetime.strptime('2021-07-01 00:05:00', '%Y-%m-%d %H:%M:%S') +
    #                                datetime.timedelta(minutes=(5 * (1 * i))),
    #                                '%Y-%m-%d %H:%M:%S') for i in range(10)]
    # for ti in last_time_list:
    #     data_gantry_ls = data[data['TIME_POINT'] == ti]
    #     data_station_ls = data_station[data_station['TIME_POINT'] == ti]
    #     compute_level_of_congestion(ti, data_station_ls, data_gantry_ls)
    while True:
        start_function_by_period()
