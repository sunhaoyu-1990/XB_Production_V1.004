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

# ����ȫ�ֱ���
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


# ����������ȡ��
class parameter:
    Gantrys = mop.load_data_from_mysql('overspeed_mysql', 'road_divide_flow_config', ['ROAD_ID'], ['G0030610030'],
                                       ['='], get_feature=['TOLL_INTERVALS']).values
    Gantrys = Gantrys[0][0].split('|')

    Stations = mop.load_data_from_mysql('overspeed_mysql', 'road_divide_flow_config', ['ROAD_ID'], ['G0030610030'],
                                        ['='], get_feature=['TOLL_INTERVALS']).values
    Stations = Stations[0][0].split('|')
    # ��ȡ�շѵ�Ԫ����һ����Ԫ�Ķ�Ӧ��ϵ�ֵ�
    gantry_back_data = dbf.get_disc_from_document('./Data_Origin/tom_noderelation.csv',
                                                  ['EXROADNODEID', 'ENROADNODEID'],
                                                  encoding='gbk', key_for_N=True, key_for_N_type='list')

    # ��ȡ�շѵ�Ԫ����һ���շ�վ�Ķ�Ӧ��ϵ�ֵ�
    last_station_dict = dbf.get_disc_from_document('./Data_Origin/tollinterval.csv',
                                                   ['id', 'enTollStation'], encoding='utf-8', key_for_N=False)

    # ��ȡ�շѵ�Ԫstart stake�ֵ�
    start_stake_dict = dbf.get_disc_from_document('./Data_Origin/tollinterval.csv', ['id', 'startStakeNum'],
                                                  encoding='utf-8', key_for_N=False, key_for_N_type='list')

    # ��ȡ�շѵ�Ԫend stake�ֵ�
    end_stake_dict = dbf.get_disc_from_document('./Data_Origin/tollinterval.csv', ['id', 'endStakeNum'],
                                                encoding='utf-8', key_for_N=False, key_for_N_type='list')

    # ��ȡ��·�ε��ٶ���ֵ
    speed_threshold_data = dbf.get_disc_from_document('./statistic_data/basic_data/gantry_speed_threshold.csv',
                                                      ['id', 'speed'], encoding='utf-8', key_for_N=False)

    # ��ȡ��ʡʡ���ż�ID��
    province_gantry = da.get_data_some_col_have_save_value(['./Data_Origin/tollinterval.csv'], ['provinceType'], ['2'],
                                                           ['='], ifIndex=False, ifSave=False, save_columns=['id'])


'''
    ����ʱ�䣺2022/8/26
    ���ʱ�䣺2022/9/29
    ���ܣ�ʵ��ӵ�³��ȼ��㺯��
    �޸�ʱ�䣺
'''


@app.post("/compute_congestion_length")
async def compute_congestion_length(INTERVAL_ID: str = Form(None, max_length=16, min_length=16),
                                    EVENT_STAKE_NUM: float = Form(None),
                                    TIME_POINT: str = Form(None, max_length=19, min_length=19)):
    """
    ʵ��ӵ�³��ȼ��㺯��
    :param TIME_POINT:
    :param EVENT_STAKE_NUM: �¼��ص�λ�õ�׮��
    :param INTERVAL_ID: �շѵ�ԪID
    :return:
    """
    # �����ṩ�ĵص㣬����ӵ�µ������εı�������
    # ��ȡ���շѵ�Ԫ����ֹ׮����Ϣ
    interval_data = dbf.get_disc_from_document('./Data_Origin/tollinterval.csv',
                                               ['id', 'startStakeNum', 'endStakeNum'],
                                               encoding='utf-8', key_for_N=False)
    # �������β��ֵı���
    rate = abs(EVENT_STAKE_NUM - float(interval_data[0])) / abs(float(interval_data[1]) - float(interval_data[0]))
    # ���м�Ȩ����
    rate = np.sin(rate * 2 / np.pi)
    rate = [rate, 1 - rate]

    # ��ȡÿ�����͵ĵ�ǰ������
    now_have_num = mop.load_data_from_mysql('overspeed_mysql', 'interval_flow', ['INTERVAL_ID', 'TIME_POINT'],
                                            [INTERVAL_ID, TIME_POINT], ['=', '='])
    # ��������ת��Ϊ�ֵ�����
    now_have_num = dbf.get_disc_from_document(now_have_num.values, [4, 7], encoding='utf-8',
                                              input_type='list')

    # ����ģ����㳵������
    LENGTH = mcm.monte_carlo_main(now_have_num, rate)

    # ������д�����ݿ�
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
    ����ʱ�䣺2022/10/19
    ���ʱ�䣺2022/10/29
    ���ܣ���ʱȷ�����������ϴ�ʱ�䣬����и��£��ͽ���ӵ�¸�֪����
    �޸�ʱ�䣺No.1 2022/10/31, update the charge if now_flow_data have 280 things
            No.2 2023/01/28�������շ�վ����������ݡ��ż�ID�����շ�վID���Ļ�ȡ���޸�������������ж�����
'''


@app.get("./start_function_by_period/")
def start_function_by_period():
    """
    �����·�θ�ʱ�ε��������������
    :return:
    """
    print('begin the update time check-------------------', time.strftime('%Y-%m-%d %H:%M:%S',
                                                                          time.localtime(time.time())))
    # ��ȡ��������������¸���ʱ��
    upload_inout_time = mop.load_data_from_mysql('overspeed_mysql', 'interval_flow_upload_time', [], [], [])

    # ������һ��ʱ���
    next_time = datetime.datetime.strftime(
        datetime.datetime.strptime(upload_inout_time.iloc[0, 0], '%Y-%m-%d %H:%M:%S') +
        datetime.timedelta(minutes=5), '%Y-%m-%d %H:%M:%S')
    print(next_time)

    #
    # ��ȡ��һ��ʱ����շѵ�Ԫ�����������
    print("get next time base data")
    now_flow_data = mop.load_data_from_mysql('overspeed_mysql', 'interval_flow_base', ['TIME_POINT'], [next_time],
                                             ['='])  # 2022/10/31
    # ��ȡ��һ��ʱ����շ�վ����������ݣ�changed by 2023/01/28
    now_station_flow_data = mop.load_data_from_mysql('overspeed_mysql', 'station_flow', ['TIME_POINT'], [next_time],
                                                     ['='])
    # �ж��Ƿ�Ϊ�״����У��״����м������еĻ�������
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
    # �жϻ�ȡ���������Ƿ�Ϊ��
    if now_flow_data.shape[0] != len(gantrys) or now_station_flow_data_ls.shape[0] != len(
            stations):  # 2023/01/28 update
        # ���û�и��£��������ȴ���һ�ο���
        print('the data is the old, continue wait')
        pass
    else:
        # ��������������и��£���������
        print('have new data, begin compute')
        compute_level_of_congestion(next_time, now_station_flow_data, now_flow_data)
    print('end the update time check-------------------', time.strftime('%Y-%m-%d %H:%M:%S',
                                                                        time.localtime(time.time())))


'''
    ����ʱ�䣺2022/8/26
    ���ʱ�䣺2022/10/14
    ���ܣ���·�θ�ʱ�ε�ӵ�µȼ���ӵ��Ԥ������
    �޸�ʱ�䣺No.1 2023/01/28,�����շ�վ��ǰʱ�����ݵ����룬�޸�·�����������ͳ��������������
'''


def compute_level_of_congestion(time_point, now_station_flow_data, now_flow_data):
    """
    �����·�θ�ʱ�ε��������������
    :param now_station_flow_data:
    :param DataFrame now_flow_data: ��ǰʱ�̵Ļ�����Ϣ���������ż��������ٶȣ�
    :param str time_point: ���м����ʱ���ֵ����ʽ��YYYY-MM-DD hh:mm:ss"
    :return:
    """
    global gantrys, stations, compute_num, old_inout_data, flow_history, new_inout_data, new_flow_history, \
        province_gantry
    print('get the basic data-----------')
    congestion_upload = 0

    # ��ȡ���շѵ�Ԫ��һʱ�εĳ�����
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

    # ���ٶ�����ת��Ϊ�ֵ�����
    now_speed_dict = dbf.get_disc_from_document(now_flow_data.values, [0, 1, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25],
                                                ifIndex=False, encoding='utf-8', key_length=2, sign='_',
                                                input_type='list')

    # ����������ת��Ϊ�ֵ�����
    now_flow_vType_dict = dbf.get_disc_from_document(now_flow_data.values, [0, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                                                     ifIndex=False, encoding='utf-8', key_length=1, sign='_',
                                                     input_type='list')

    # ���շ�վ��������ת��Ϊ�ֵ�����
    now_station_flow_vType_dict = dbf.get_disc_from_document(now_station_flow_data.values,
                                                             [0, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                                                             ifIndex=False, encoding='utf-8', key_length=3, sign='_',
                                                             input_type='list')
    # ������շѵ�Ԫ·�ε�ǰʱ���������������ͳ�����, change:2023/01/28
    print("compute the inout num and have num:-----------start")
    gantry_have_data, gantry_inout_data, gantry_have_total_data, gantry_inout_total_data = \
        pc.compute_num_of_gantry(now_flow_vType_dict, now_station_flow_vType_dict, last_have_dict, gantry_back_data,
                                 last_station_dict, time_point, province_gantry, gantrys=gantrys)
    print("compute the inout num and have num:-----------end")

    if compute_num == 0:
        # ��ȡ���շѵ�Ԫ·�ε�ǰʱ����ǰ13��ʱ�ε�������������
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

        # ��ȡ���շѵ�Ԫ·�ε�ǰʱ����ǰ48��ʱ�ε���������
        print("get 48 flow of gantrys before this time from mysql:-----------start")
        last_time_list = [datetime.datetime.strftime(datetime.datetime.strptime(time_point, '%Y-%m-%d %H:%M:%S') +
                                                     datetime.timedelta(minutes=(5 * (-1 * (j + 1)))),
                                                     '%Y-%m-%d %H:%M:%S') for j in range(48)]
        print("flow data last_time_list data num:" + str(len(last_time_list)))
        flow_history = mop.load_data_from_mysql('overspeed_mysql', 'interval_flow_base', ['TIME_POINT'],
                                                [last_time_list], ['in'])
        print("flow_history data num:" + str(round(flow_history.shape[0] / (len(gantrys) * 10), 1)))
        print("get 48 flow of gantrys before this time from mysql:-----------end")
        # ����13��ʱ�������������ݵ�ת��
        if old_inout_data.empty:
            pass
        else:
            old_inout_data = old_inout_data[['INTERVAL_ID', 'TIME_POINT', 'IN_TOTAL', 'OUT_TOTAL']]
            old_inout_data = old_inout_data.sort_values(['TIME_POINT'])
        # ����48��ʱ���������ݵ�ת��
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
        # ��ȡ���շѵ�Ԫ·�ε�ǰʱ����ǰ13��ʱ�ε�������������
        print("update 13 inout num of gantrys before this time from mysql:-----------start")
        old_inout_data = pd.concat((old_inout_data, new_inout_data))
        print("old_inout_data data total num:" + str(old_inout_data.shape[0]))
        # print("old_inout_data data num:" + str(round(old_inout_data.shape[0]/(len(gantrys) * 10),1)))
        print("update 13 inout num of gantrys before this time from mysql:-----------end")

        # ��ȡ���շѵ�Ԫ·�ε�ǰʱ����ǰ48��ʱ�ε���������
        print("update 48 flow of gantrys before this time from mysql:-----------start")
        flow_history = pd.concat((flow_history, new_flow_history))
        print("flow_history data num:" + str(round(flow_history.shape[0] / (len(gantrys) * 10), 1)))
        print("update 48 flow of gantrys before this time from mysql:-----------end")

    level_result = {}
    # gantry_inout_data_new = dbf.compute_dict_by_group(gantry_inout_data, [0, 2], 'sum', '_')
    gantry_inout_data_new = gantry_inout_total_data
    # gantry_have_data_new = dbf.compute_dict_by_group(gantry_have_data, [0], 'sum', '_')
    gantry_have_data_new = gantry_have_total_data  # changed at 2023/01/29

    # ��ʼ����ӵ�µȼ��ж�
    if old_inout_data.empty:  # ���û�������������ʷ���ݣ�ֱ������
        old_inout_data = pd.DataFrame(data=None, columns=['INTERVAL_ID', 'TIME_POINT', 'IN_TOTAL', 'OUT_TOTAL'])
        pass
    else:
        flow_history_dict = flow_history.groupby(['INTERVAL_ID', 'VEHICLE_TYPE'])['FLOW'].apply(
            lambda x: list(x)).reset_index()
        old_inout_data_ls = old_inout_data.groupby(['INTERVAL_ID'])['IN_TOTAL'].count().reset_index()
        # �����ʷ��������������Ҫ�󣬼�����ӵ�µȼ��ж�
        if len(flow_history_dict.iloc[0, 2]) < 48 or old_inout_data_ls.iloc[0, 1] < 13:
            pass
        else:
            print('begin congestion level compute-------------------------------------------------')
            flow_history_dict = dbf.get_disc_from_document(flow_history_dict.values, [0, 1, 2], key_length=2, sign='_',
                                                           ifIndex=False, key_for_N=False, encoding='utf-8',
                                                           input_type='list')
            congestion_upload = 1
            # ��������������ת��Ϊ�ֵ�
            old_inout_data_in = dbf.get_disc_from_document(old_inout_data.values, [0, 2], ifIndex=False, key_for_N=True,
                                                           key_for_N_type='list', encoding='utf-8',
                                                           input_type='list')
            old_inout_data_out = dbf.get_disc_from_document(old_inout_data.values, [0, 3], encoding='utf-8',
                                                            key_for_N=True, key_for_N_type='list', ifIndex=False,
                                                            input_type='list')

            # ͨ���ٶ��������ӵ���ж�
            print('congestion level compute by speed:-----------start')
            congestion_charge_speed = pc.charge_congestion_with_speed(now_speed_dict, time_point, now_flow_vType_dict,
                                                                      speed_threshold_data, gantrys=gantrys)
            print("result count num:" + str(len(congestion_charge_speed)))
            print('congestion level compute by speed:-----------end')

            # ���и�·��������������������ƶȼ��㣬�жϸ�·���Ƿ��ѷ���ӵ��
            print('congestion level compute by flow:-----------start')
            congestion_charge_flow = pc.charge_congestion_with_flow_curve(old_inout_data_in, old_inout_data_out,
                                                                          gantry_inout_data_new, gantry_back_data,
                                                                          gantrys=gantrys)
            print("result count num:" + str(len(congestion_charge_flow)))
            print('congestion level compute by flow:-----------end')

            # ����Ŀǰ��·�εĳ�����������Сʱ�������������·�γ�����ֵ�����жԱ�
            print('congestion level compute by have num:-----------start')
            congestion_charge_have = pc.charge_congestion_by_haveNum(gantry_have_data_new, time_point,
                                                                     flow_history_dict, gantry_back_data,
                                                                     last_station_dict, province_gantry,
                                                                     gantrys=gantrys)
            print("result count num:" + str(len(congestion_charge_have)))
            print('congestion level compute by have num:-----------end')

            # ����Ŀǰ��·�εĳ�����������ӵ�µȼ��ж�
            print('congestion level result put out:-----------start')
            level_result = pc.charge_congestion_level_by_result(congestion_charge_speed, congestion_charge_flow,
                                                                congestion_charge_have, gantry_have_data, time_point)
            print("result count num:" + str(len(level_result)))
            print('congestion level result put out:-----------end')
            print('end congestion level compute----------------------------------------------------')
            # ɾ�����������ʷ�����е���������
            last_time_list = datetime.datetime.strftime(datetime.datetime.strptime(time_point, '%Y-%m-%d %H:%M:%S') +
                                                        datetime.timedelta(minutes=(-65)), '%Y-%m-%d %H:%M:%S')
            old_inout_data = old_inout_data[old_inout_data['TIME_POINT'] != last_time_list]

            # ɾ�������������ʷ����
            flow_history = flow_history.iloc[len(new_flow_history), :]

    # ������ʱ�̵ĸ�������������
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

    # ���н�������ݿ��ϴ�
    # if the upload time equal this time
    print('begin mysql database upload------------------------------------------------------')
    upload_time = mop.load_data_from_mysql('overspeed_mysql', 'interval_flow_upload_time', [], [], [])
    last_time = datetime.datetime.strftime(datetime.datetime.strptime(time_point, '%Y-%m-%d %H:%M:%S') +
                                           datetime.timedelta(minutes=(-5)), '%Y-%m-%d %H:%M:%S')
    if upload_time.empty or last_time == upload_time.iloc[0, 0]:
        # ������д�����ݿ�
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
        # ������ʱ�̵ĸ�·�������������
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
            # ӵ�µȼ�����ϴ�
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
    ����ʱ�䣺2022/10/19
    ���ʱ�䣺2022/10/19
    ���ܣ�������
    �޸�ʱ�䣺
'''


def main(period_time):
    """
    ������
    :param int period_time: ��ʱѭ����ʱ��������λ��
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
