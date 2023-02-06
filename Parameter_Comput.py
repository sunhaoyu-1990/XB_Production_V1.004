# coding=gbk
"""
ӵ��ģ������ؼ������ļ���
�ĵ�����ʱ�䣺2022/7/14
�ĵ��޸�ʱ�䣺
"""
import csv
import datetime
import math
import pickle
# import time

# import numpy as np
# import pandas as pd
import neuralNetwork
import Monte_Carlo_method as mcm
import Data_analysis_2 as da2
import Keyword_and_Parameter as kp
import Data_Basic_Function as dbf
import Document_process as dop

# from chinese_calendar import is_workday

'''
    ����ʱ��: 2022/7/14
    ���ʱ��: 2022/7/14
    ����: �����ṩ��·���ż�ID��ʱ��ȥ��ȡǰ���żܼ��շ�վ����������
'''


def get_all_flow_of_gantry(last_gantry, last_sta, next_gantry, next_sta, start_time, goal_time):
    """
    �����ṩ��·���ż�ID��ʱ��ȥ��ȡǰ���żܼ��շ�վ����������
    :param goal_time: Ԥ���ʱ��
    :param start_time: �¼���ʼʱ��
    :param last_gantry: �¼�·�ε���һ���ż�ID
    :param last_sta: �¼�·�ε���һ���շ�վ
    :param next_gantry: �¼�·�ε���һ���ż�ID
    :param next_sta: �¼�·�ε���һ���շ�վ
    :return:
    """
    # ��ȡ��ǰ׷������Сʱ������
    before_start_time = datetime.datetime.strftime(
        datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=(-2)),
        '%Y-%m-%d %H:%M:%S')
    before_start_date = before_start_time[:10]
    # ��ȡ�¼���������������
    start_date = start_time[:10]
    # ��ȡԤ��ʱ����Ӧ������
    goal_date = datetime.datetime.strftime(
        datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=(goal_time)),
        '%Y-%m-%d %H:%M:%S')[:10]
    # ��ȡ���¼�����ǰ����Сʱ���¼�����ʱ�̸��ż��շ�վ��ȫ����������
    last_gantry_before_data = []  # ���ڴ洢�¼�ǰ����һ���żܸ���������
    last_sta_in_before_data = []  # ���ڴ洢�¼�ǰ����һ���շ�վ��վ�־����¼�·���żܵĸ���������
    last_sta_out_before_data = []  # ���ڴ洢�¼�ǰ����һ���շ�վ��վ�Ҿ�����һ���żܵĸ���������

    gantry_type_flow_path = kp.get_parameter_with_keyword('gantry_type_flow_path')
    station_out_flow_path = kp.get_parameter_with_keyword('station_out_flow_path')
    station_in_flow_path = kp.get_parameter_with_keyword('station_in_flow_path')
    if before_start_date == start_date:
        pass
    else:
        with open(before_start_date + start_date + '.csv') as f:
            for i, row in enumerate(f):
                row = row.split(',')
                row[-1] = row[-1][:-1]
                if row[0] >= before_start_time and row[2] == last_gantry:
                    last_gantry_before_data.append(row)
        with open(before_start_date + start_date + '.csv') as f:
            for i, row in enumerate(f):
                row = row.split(',')
                row[-1] = row[-1][:-1]
                if row[0] >= before_start_time and row[2] == last_gantry:
                    station_out_flow_path.append(row)
    with open(gantry_type_flow_path + start_date + '.csv') as f:
        for i, row in enumerate(f):
            row = row.split(',')
            row[-1] = row[-1][:-1]

    next_gantry_before_data = []  # ���ڴ洢�¼�ǰ����һ���żܸ���������
    next_sta_in_before_data = []  # ���ڴ洢�¼�ǰ����һ���żܸ���������


'''
    ����ʱ��: 2022/8/1
    ���ʱ��: 2022/8/1
    ����: �������λ�ã�ͳ�Ʊ��뾭��ĳһ�˵���������
'''


def get_flow_num_of_flyover(paths):
    """
    �������λ�ã�ͳ�Ʊ��뾭��ĳһ�˵���������
    :param paths: ��ַ��
    :return:
    """
    data_result = {}
    # �������������ż�ID����
    xi_road_left = ["G003061003000810", "G003061003000820", "G003061003000910", "G003061003000920", "G003061003001010",
                    "G003061003001020", "G003061003001110", "G003061003001120", "G003061003001210",
                    "G003061003001220", "G003061003001310", "G003061003001320", "G003061003001410", "G003061003001420",
                    "G003061003001510", "G003061003001520", "G003061003001610", "G003061003001620", "G003061003001710",
                    "G003061003001720"]
    xi_road_right = ["G003061003000610", "G003061003000620", "G003061003000710", "G003061003000720", "G003061003000510",
                     "G003061003000520"]
    xi_road_down = ["G003061003000310", "G003061003000320", "G003061003000210", "G003061003000220", "G030N61001001510",
                    "G030N61001001520"]
    # ��ƽ�����ϵ��ż�ID
    direction4 = ['G003061003000810']
    # ��������-ï�귽���ϵ��ż�ID
    direction1 = ['G030N61001001420']
    # ��μ��ͨ�����ϵ��ż�ID
    direction2 = ['G003061003000320', 'G030N61001001510', 'G003061003000220']
    # �������η�����ż�ID
    direction3 = ['G003061003000720']
    for path in paths:
        print(path)
        with open(path) as f:
            for i, row in enumerate(f):
                row = row.split(',')
                row[-1] = row[-1][:-1]
                if i == 0:
                    col_index = dbf.get_indexs_of_list(row, ['�շѵ�Ԫ·��'])
                else:
                    # �������Ϊï�귽��
                    if direction1[0] in row[col_index[0]]:
                        # ��ȡ���ż�֮ǰ�������ż��б�
                        col_list = row[col_index[0]].split('|')
                        index = dbf.get_indexs_of_list(col_list, direction1[0])
                        col_set = set(col_list[(index - 6):index])
                        # ��������żܺ���������·���н�����˵������ƽ����
                        if len(col_set.intersection(xi_road_left)) > 0:
                            try:
                                data_result['G003061003000820'] += 1
                            except:
                                data_result['G003061003000820'] = 1
                        # ��������żܺ������Ұ��·���н�����˵��������������
                        elif len(col_set.intersection(xi_road_right)) > 0:
                            try:
                                data_result['G003061003000710'] += 1
                            except:
                                data_result['G003061003000710'] = 1
                        # ��������żܺ������°��·���н�����˵���������Ϲ���
                        elif len(col_set.intersection(xi_road_down)) > 0:
                            try:
                                data_result['G003061003000310'] += 1
                            except:
                                data_result['G003061003000310'] = 1
                    # �������Ϊ����������
                    if direction3[0] in row[col_index[0]]:
                        # ��ȡ���ż�֮ǰ�������ż��б�
                        col_list = row[col_index[0]].split('|')
                        index = dbf.get_indexs_of_list(col_list, direction1[0])
                        col_set = set(col_list[(index - 6):index])
                        # ��������żܺ�����·���н�����˵������ƽ����
                        if len(col_set.intersection(xi_road_left)) > 0:
                            try:
                                data_result['G003061003000820'] += 1
                            except:
                                data_result['G003061003000820'] = 1
                        # ��������żܺ������°��·���н�����˵���������Ϲ���
                        elif len(col_set.intersection(xi_road_down)) > 0:
                            try:
                                data_result['G003061003000310'] += 1
                            except:
                                data_result['G003061003000310'] = 1

                    # �������Ϊ����������
                    if direction4[0] in row[col_index[0]]:
                        # ��ȡ���ż�֮ǰ�������ż��б�
                        col_list = row[col_index[0]].split('|')
                        index = dbf.get_indexs_of_list(col_list, direction1[0])
                        col_set = set(col_list[(index - 6):index])
                        # ��������żܺ�����·���н�����˵������ƽ����
                        if len(col_set.intersection(xi_road_right)) > 0:
                            try:
                                data_result['G003061003000710'] += 1
                            except:
                                data_result['G003061003000710'] = 1
                        # ��������żܺ������°��·���н�����˵���������Ϲ���
                        elif len(col_set.intersection(xi_road_down)) > 0:
                            try:
                                data_result['G003061003000310'] += 1
                            except:
                                data_result['G003061003000310'] = 1

                    # �������Ϊ��μ��������
                    for j in range(len(direction2)):
                        if direction2[j] in row[col_index[0]]:
                            # ��ȡ���ż�֮ǰ�������ż��б�
                            col_list = row[col_index[0]].split('|')
                            index = dbf.get_indexs_of_list(col_list, direction1[0])
                            col_set = set(col_list[(index - 6):index])
                            # ��������żܺ�����·���н�����˵������ƽ����
                            if len(col_set.intersection(xi_road_left)) > 0:
                                try:
                                    data_result['G003061003000820'] += 1
                                except:
                                    data_result['G003061003000820'] = 1
                            # ��������żܺ������Ұ��·���н�����˵��������������
                            elif len(col_set.intersection(xi_road_right)) > 0:
                                try:
                                    data_result['G003061003000710'] += 1
                                except:
                                    data_result['G003061003000710'] = 1
                            break
    return data_result


'''
    ����ʱ��: 2022/8/2
    ���ʱ��: 2022/8/2
    ����: ��ȡ�����żܾ��м�¼�ĳ��������żܾ���ʱ�䡢����
'''


def get_data_of_two_gantry_have(paths, gantry_id, corraletion='and', result_type='list'):
    """
    ��ȡ�����żܾ��м�¼������
    :param corraletion:
    :param result_type:
    :param gantry_id: �����ż�ID������ͨ������żܣ�ͬʱͨ�����������żܵĳ���
    :param paths: ��ַ��
    :return:
    """
    # ���ݱ���
    if result_type == 'list':
        data_result = []
    elif result_type == 'dict':
        data_result = {}
    for path in paths:
        print(path)
        with open(path) as f:
            for i, row in enumerate(f):
                row = row.split(',')
                row[-1] = row[-1][:-1]
                if corraletion == 'or':
                    if i == 0:
                        col_index = dbf.get_indexs_of_list(row, ['�ż�·��', '�ż�ʱ�䴮', '���ڳ���', '��ڳ���'])
                    else:
                        for gantry in gantry_id:
                            if gantry in row[col_index[0]]:
                                # ��ȡ�ż��б�
                                gantry_list = row[col_index[0]].split('|')
                                # ��ȡ�ż�ʱ���б�
                                time_list = row[col_index[1]].split('|')
                                # ��ȡ��ǰ�żܵ��±�
                                gantry_index = dbf.get_indexs_of_list(gantry_list, [gantry])
                                # ��ȡ��ǰ���żܵľ���ʱ��
                                gantry_time = time_list[gantry_index[0]]
                                # ȷ������
                                if row[col_index[2]] != '':
                                    vehicle_type = row[col_index[2]]
                                else:
                                    vehicle_type = row[col_index[3]]
                                # ȷ��ʱ������ʱ��
                                if '5' > gantry_time[-4] >= '0':
                                    gantry_time = gantry_time[:-4] + '0:00'
                                else:
                                    gantry_time = gantry_time[:-4] + '5:00'

                                if result_type == 'list':
                                    data_result.append([gantry, gantry_time, vehicle_type])
                                elif result_type == 'dict':
                                    try:
                                        data_result[gantry + '_' + gantry_time + '_' + vehicle_type] += 1
                                    except:
                                        data_result[gantry + '_' + gantry_time + '_' + vehicle_type] = 1
                elif corraletion == 'and':
                    if len(gantry_id) >= 2:
                        if i == 0:
                            col_index = dbf.get_indexs_of_list(row, ['�ż�·��', '�ż�ʱ�䴮', '���ڳ���', '��ڳ���'])
                        else:
                            if gantry_id[1] in row[col_index[0]] and gantry_id[0] in row[col_index[0]]:
                                # ��ȡ�ż��б�
                                gantry_list = row[col_index[0]].split('|')
                                # ��ȡ�ż�ʱ���б�
                                time_list = row[col_index[1]].split('|')
                                # ��ȡ�����żܵ��±�
                                last_gantry_index = dbf.get_indexs_of_list(gantry_list, [gantry_id[1]])
                                # ��ȡ��ǰ�żܵ��±�
                                gantry_index = dbf.get_indexs_of_list(gantry_list, [gantry_id[0]])
                                # ��ȡ�����żܵľ���ʱ��
                                last_gantry_time = time_list[last_gantry_index[0]]
                                # ��ȡ��ǰ���żܵľ���ʱ��
                                gantry_time = time_list[gantry_index[0]]
                                # ȷ������
                                if row[col_index[2]] != '':
                                    vehicle_type = row[col_index[2]]
                                else:
                                    vehicle_type = row[col_index[3]]
                                # ȷ��ʱ������ʱ��
                                if '5' > gantry_time[-4] >= '0':
                                    gantry_time = gantry_time[:-4] + '0:00'
                                else:
                                    gantry_time = gantry_time[:-4] + '5:00'

                                if '5' > last_gantry_time[-4] >= '0':
                                    last_gantry_time = last_gantry_time[:-4] + '0:00'
                                else:
                                    last_gantry_time = last_gantry_time[:-4] + '5:00'

                                data_result.append(
                                    [gantry_id[0], gantry_id[1], gantry_time, last_gantry_time, vehicle_type])

                            if gantry_id[2] in row[col_index[0]] and gantry_id[0] in row[col_index[0]]:
                                # ��ȡ�ż��б�
                                gantry_list = row[col_index[0]].split('|')
                                # ��ȡ�ż�ʱ���б�
                                time_list = row[col_index[1]].split('|')
                                # ��ȡ�����żܵ��±�
                                next_gantry_index = dbf.get_indexs_of_list(gantry_list, [gantry_id[2]])
                                # ��ȡ��ǰ�żܵ��±�
                                gantry_index = dbf.get_indexs_of_list(gantry_list, [gantry_id[0]])
                                # ��ȡ�����żܵľ���ʱ��
                                next_gantry_time = time_list[next_gantry_index[0]]
                                # ��ȡ��ǰ���żܵľ���ʱ��
                                gantry_time = time_list[gantry_index[0]]
                                # ȷ������
                                if row[col_index[2]] != '':
                                    vehicle_type = row[col_index[2]]
                                else:
                                    vehicle_type = row[col_index[3]]
                                data_result.append(
                                    [gantry_id[0], gantry_id[2], gantry_time, next_gantry_time, vehicle_type])

    # with open('./data_check/flow_check/' + gantry_id[0] + '.csv', 'w', newline='') as f:
    #     writer = csv.writer(f)
    #     writer.writerows(data_result)

    return data_result


'''
    ����ʱ��: 2022/8/3
    ���ʱ��: 2022/8/3
    ����: ����ͬʱ���������żܵ���Ϣ������ռ��ͳ��
'''


def statistic_the_two_gantry_data(data):
    """
    ����ͬʱ���������żܵ���Ϣ������ռ��ͳ��
    :param data:
    :return:
    """
    # ���ڱ��棬�����żܵĸ�ʱ�θ���������
    this_gantry = {}
    other_gantry = {}

    for i in range(len(data)):
        try:
            this_gantry[data[i][0] + '_' + data[i][1] + '_' + data[i][2] + '_' + data[i][4]] += 1
        except:
            this_gantry[data[i][0] + '_' + data[i][1] + '_' + data[i][2] + '_' + data[i][4]] = 1
        try:
            other_gantry[data[i][0] + '_' + data[i][1] + '_' + data[i][2] + '_' + data[i][3] + '_' + data[i][4]] += 1
        except:
            other_gantry[data[i][0] + '_' + data[i][1] + '_' + data[i][2] + '_' + data[i][3] + '_' + data[i][4]] = 1

    for key in other_gantry.keys():
        key_list = key.split('_')
        other_gantry[key_list[0] + '_' + key_list[1] + '_' + key_list[2] + '_' + key_list[3] + '_' + key_list[4]] = \
            other_gantry[key_list[0] + '_' + key_list[1] + '_' + key_list[2] + '_' + key_list[3] + '_' + key_list[4]] / \
            this_gantry[key_list[0] + '_' + key_list[1] + '_' + key_list[2] + '_' + key_list[4]]

    return other_gantry, this_gantry


'''
    ����ʱ��: 2022/8/4
    ���ʱ��: 2022/8/4
    ����: ����У���żܵĳ������������θ�ʱ�ε�ռ�ȣ�������������
'''


def get_real_flow_pass_gantry(gantry_id):
    """
    ����У���żܵĳ������������θ�ʱ�ε�ռ�ȣ�������������
    :param gantry_id:
    :return:
    """
    # �Ȼ�ȡ��ǰ���������żܵ�ʵ������
    # ��ȡ��׼·���������ֵ�����
    standard_dict = dbf.get_disc_from_document('../Data_Origin/tom_noderelation.csv', ['ENROADNODEID', 'EXROADNODEID'],
                                               encoding='gbk', key_for_N=True, key_for_N_type='list')
    # ��ȡ��׼·���ķ����ֵ�����
    standard_back_dict = dbf.get_disc_from_document('../Data_Origin/tom_noderelation.csv',
                                                    ['EXROADNODEID', 'ENROADNODEID'],
                                                    encoding='gbk', key_for_N=True, key_for_N_type='list')
    # ��ȡ��׼·���ķ����ֵ�����
    last_station_dict = dbf.get_disc_from_document('../Data_Origin/tollinterval.csv', ['id', 'enTollStation'],
                                                   encoding='gbk', key_for_N=True, key_for_N_type='list')
    # ��ȡ��׼·���ķ����ֵ�����
    next_station_dict = dbf.get_disc_from_document('../Data_Origin/tollinterval.csv', ['id', 'exTollStation'],
                                                   encoding='gbk', key_for_N=True, key_for_N_type='list')
    # ��ȡ�������ż�ID
    last_gantry = standard_back_dict[gantry_id]
    next_gantry = standard_dict[gantry_id]

    # ��ȡ��ǰ�ż�ID������
    gantry_flow = get_data_of_two_gantry_have('', [gantry_id], 'or', 'dict')
    # ��ȡ�����ż�ID������
    last_gantry_flow = get_data_of_two_gantry_have('', [last_gantry], 'or', 'dict')
    # ��ȡ�����ż�ID������
    next_gantry_flow = get_data_of_two_gantry_have('', [next_gantry], 'or', 'dict')

    # ��ȡ�������żܸ���ʱ�θ���������
    gantry_data_list = get_data_of_two_gantry_have('', [gantry_id, last_gantry, next_gantry], 'and', 'list')
    # ��ȡ�����θ�ʱ�θ�����ռ��
    rate_gantry_flow, this_gantry = statistic_the_two_gantry_data(gantry_data_list)

    # ��ȡ��һ���շ�վ��ID
    last_station = last_station_dict[gantry_id]
    # ��ȡ��һ���շ�վ��ID
    next_station = next_station_dict[gantry_id]

    # ��ȡ��һ���շ�վ����������
    last_station_list = da2.get_station_flow('', last_station, gantry_id[:-2], 'dict')

    # ��ȡ��һ���շ�վ����������
    next_station_list = da2.get_station_flow('', next_station, gantry_id[:-2], 'dict')

    for key in gantry_flow.keys():
        key_list = key.split('_')
        origin_num = gantry_flow[key]
        last_station_num = last_station[last_station + '_' + key_list[0]]


'''
    ����ʱ�䣺2022/8/5
    ���ʱ�䣺2022/8/5
    ���ܣ�����ʻ��¼��·����ʱ�䴮���в�֣���������Ҫ��ĸ�ʽ���з��أ������͸��ڵ�֮���ʱ�伯��
    �޸�ʱ�䣺
'''


def get_list_of_gantry_path(paths, gantry_name, time_name, vehicleInType_name, vehicleOutType_name, return_type='dict'):
    """
    ����ʻ��¼��·����ʱ�䴮���в�֣���������Ҫ��ĸ�ʽ���з��أ������͸��ڵ�֮���ʱ�伯��
    :param vehicleOutType_name: ���ڳ������ڵ��ֶ�����
    :param paths: ��ַ��
    :param gantry_name: �ż�·�����ڵ��ֶ�����
    :param time_name: �ż�ʱ�䴮���ڵ��ֶ�����
    :param vehicleInType_name: ��ڳ������ڵ��ֶ�����
    :param return_type: ���ص��������ͣ�listΪ���ݣ�dictΪ�ֵ䣬save��Ϊ���ر���
    :return:
    """
    # ��ÿһ����ʻ���ż�·����ʱ����в��
    if return_type == 'dict':
        data_list = {}
    else:
        data_list = []
    for path in paths:
        print(path)
        with open(path) as f:
            for i, row in enumerate(f):
                row = row.split(',')
                row[-1] = row[-1][:-1]
                if i == 0:
                    index = dbf.get_indexs_of_list(row,
                                                   [gantry_name, time_name, vehicleInType_name, vehicleOutType_name])
                else:
                    if '|' in row[index[0]]:
                        gantry_list = row[index[0]].split('|')  # ����ż�·��
                    else:
                        continue
                    if '|' in row[index[1]]:
                        time_list = row[index[1]].split('|')  # ����ż�ʱ��
                    else:
                        continue
                    if row[index[3]] != '':
                        vehicle_type = row[index[3]]
                    else:
                        vehicle_type = row[index[2]]

                    # ѭ�������ż�����
                    for j in range(len(gantry_list)):
                        if j <= len(gantry_list) - 2:
                            # ����ÿ��·�ż�֮��ļ��ʱ��
                            intervel = round(dbf.compute_intervel_of_two_time(time_list[j], time_list[j + 1], 'minite'),
                                             0)

                            if return_type == 'dict':
                                try:
                                    data_list[gantry_list[j] + '_' + gantry_list[j + 1] + '_' + vehicle_type].append(
                                        intervel)
                                except:
                                    data_list[gantry_list[j] + '_' + gantry_list[j + 1] + '_' + vehicle_type] = [
                                        intervel]

                            else:
                                data_list.append([gantry_list[j], gantry_list[j + 1], vehicle_type, intervel])

    if return_type == 'save':
        with open('./4.data_check/cangestion/gantry_type_time_length_origin_total_data.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(data_list)
    else:
        return data_list


'''
    ����ʱ�䣺2022/8/5
    ���ʱ�䣺2022/8/5
    ���ܣ���ø����͸��ڵ�֮��ͨ��ʱ��ĸ�ͳ��ֵ
    �޸�ʱ�䣺
'''


def statistic_time_between_gantrys(data_list):
    """
    ��ø����͸��ڵ�֮��ͨ��ʱ��ĸ�ͳ��ֵ
    :param data_list:
    :return:
    """
    # ���н�����ݵı���
    data = [['��Сֵ', '�ķ�֮һֵ', '��λֵ', '�ķ�֮��ֵ', '���ֵ', 'ƽ��ֵ', '����ֵ', '������', 'key']]
    for key in data_list.keys():
        time_list = data_list[key]
        result = dbf.get_statistics_value_of_list(time_list, sortBySort=True)
        result.extend([len(time_list), key])
        data.append(result)

    with open('./4.data_check/cangestion/gantry_type_time_statistic_data.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data)


'''
    ����ʱ�䣺2022/8/5
    ���ʱ�䣺2022/8/5
    ���ܣ����շѵ�Ԫ��¼����ʱ�䲹����
    �޸�ʱ�䣺
'''


def get_real_flow_with_time_revise(paths, if_return=False, treat_type='statistic', filter_id=''):
    """
    ���շѵ�Ԫ��¼����ʱ�䲹��
    :param if_return: ѡ���Ƿ���н�����ݵĵ���
    :param paths:
    :return:
    """
    # ��ȡ��׼·���ķ����ֵ�����
    standard_dict = dbf.get_disc_from_document('../Data_Origin/tom_noderelation.csv',
                                               ['ENROADNODEID', 'EXROADNODEID'],
                                               encoding='gbk', key_for_N=True, key_for_N_type='list')
    # ��ȡ��·����ʻʱ������
    standard_time_dict = dbf.get_disc_from_document('../Data_Origin/gantry_type_time_statistic_data.csv',
                                                    ['key', '����ֵ'], encoding='utf-8')
    if treat_type == 'statistic':
        data_result = {}  # ������żܵ�����ֵ
    else:
        data_result = {}
    for path in paths:

        with open(path) as f:
            if path[-8:-4] == '0421' or path[-8:-4] == '0422':
                continue
            print(path)
            for i, row in enumerate(f):
                row = row.split(',')
                row[-1] = row[-1][:-1]
                if i == 0:
                    col_index = dbf.get_indexs_of_list(row, ['GANTRYID', 'TRANSTIME', 'TOLLINTERVALID', 'VEHICLETYPE',
                                                             'PASSID', 'ENTOLLSTATIONHEX'])
                else:
                    # ���շѵ�Ԫ���в��
                    interval_list = row[col_index[2]].split('|')

                    # ���|�������շѵ�Ԫ�ֶ��У�˵�����ڲ�������
                    if filter_id == '':
                        pass
                    else:
                        if set(filter_id).intersection(set(interval_list)):
                            # if filter_id in row[col_index[2]]:
                            pass
                        else:
                            continue

                    # �Ƚ�����ʱ���ֶε�����
                    if len(row[col_index[1]]) <= 19:
                        data_time = row[col_index[1]]
                    else:
                        data_time = row[col_index[1]][:19]

                    if '|' in row[col_index[2]]:
                        # �����շѵ�Ԫ����
                        for interval in interval_list:
                            if filter_id == '':
                                pass
                            else:
                                if interval in filter_id:
                                    pass
                                else:
                                    continue

                            # ����շѵ�Ԫ�͸�����¼���ż�IDǰ16λһ�£���ֱ�Ӽ�¼����
                            if interval == row[col_index[0]][:16]:
                                # ʱ���ת��
                                if '5' > data_time[-4] >= '0':
                                    data_time = data_time[:-4] + '0:00'
                                else:
                                    data_time = data_time[:-4] + '5:00'
                                # ������������
                                if treat_type == 'statistic':
                                    try:
                                        data_result[row[col_index[0]][:16] + '_' + data_time + '_' + str(
                                            float(row[col_index[3]]))] += 1
                                    except:
                                        data_result[row[col_index[0]][:16] + '_' + data_time + '_' + str(
                                            float(row[col_index[3]]))] = 1
                                else:
                                    row[col_index[1]] = data_time
                                    # data_result.append(dbf.get_values_of_list(row, col_index))
                                    data_result[row[col_index[4]]] = [row[col_index[0]], row[col_index[1]],
                                                                      row[col_index[2]], row[col_index[3]],
                                                                      row[col_index[5]]]
                            # ����շѵ�Ԫ�������¼���ż�ID��һ��
                            else:
                                # ���иõ�Ԫ���żܵ�ʱ�����
                                try:
                                    gap = set(standard_dict[interval]).intersection(set(interval_list))
                                except:
                                    continue
                                if gap:
                                    interval_min = gap.pop()
                                else:
                                    # if len(standard_dict[interval]) <= 2:
                                    #     interval_min = min(standard_dict[interval])
                                    # else:
                                    # print((interval_list, row[col_index[0]][:16]))
                                    continue
                                # �����һ�����Ǹ��ż�
                                if interval_min == row[col_index[0]][:16]:
                                    # ƥ�������ż�֮�����ʻʱ��
                                    try:
                                        go_time = float(standard_time_dict[
                                                            interval + '_' + row[col_index[0]][:16] + '_' + row[
                                                                col_index[3]] + '.0'])
                                    except:
                                        go_time = 5.0
                                    # ����ȥ����ʱ����ʱ���
                                    data_time = datetime.datetime.strftime(
                                        datetime.datetime.strptime(data_time, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(
                                            minutes=go_time * (- 1)), '%Y-%m-%d %H:%M:%S')
                                    # ʱ���ת��
                                    if '5' > data_time[-4] >= '0':
                                        data_time = data_time[:-4] + '0:00'
                                    else:
                                        data_time = data_time[:-4] + '5:00'
                                    # ������������
                                    if treat_type == 'statistic':
                                        try:
                                            data_result[
                                                interval + '_' + data_time + '_' + str(float(row[col_index[3]]))] += 1
                                        except:
                                            data_result[
                                                interval + '_' + data_time + '_' + str(float(row[col_index[3]]))] = 1
                                    else:

                                        row[col_index[1]] = data_time
                                        # data_result.append(dbf.get_values_of_list(row, col_index))
                                        data_result[row[col_index[4]]] = [row[col_index[0]], row[col_index[1]],
                                                                          row[col_index[2]], row[col_index[3]],
                                                                          row[col_index[5]]]
                                # �����һ�����Ǹ��ż�
                                else:
                                    # ѭ����ȡ����һ���ż�֮��ĺ�ʱ��ֱ����һ���ż��Ǹ�����¼���ż�
                                    time_list = []
                                    ii = 0
                                    while True:
                                        # �ж��Ƿ���һ���ż��Ǹ����¼���ż�,����Ǿͽ���ʱ����ܣ��������������ݣ������һֱ��ȡ��ʻʱ��
                                        if interval_min == row[col_index[0]][:16]:
                                            try:
                                                go_time = float(standard_time_dict[
                                                                    interval_new + '_' + interval_min + '_' + row[
                                                                        col_index[3]] + '.0'])
                                            except:
                                                go_time = 5.0
                                            go_time = sum(time_list) + go_time
                                            # ����ȥ����ʱ����ʱ���
                                            data_time = datetime.datetime.strftime(datetime.datetime.strptime(data_time,
                                                                                                              '%Y-%m-%d %H:%M:%S') + datetime.timedelta(
                                                minutes=go_time * (- 1)), '%Y-%m-%d %H:%M:%S')
                                            # ʱ���ת��
                                            if '5' > data_time[-4] >= '0':
                                                data_time = data_time[:-4] + '0:00'
                                            else:
                                                data_time = data_time[:-4] + '5:00'
                                            # ������������
                                            if treat_type == 'statistic':
                                                try:
                                                    data_result[interval + '_' + data_time + '_' + str(
                                                        float(row[col_index[3]]))] += 1
                                                except:
                                                    data_result[interval + '_' + data_time + '_' + str(
                                                        float(row[col_index[3]]))] = 1
                                            else:
                                                row[col_index[1]] = data_time
                                                # data_result.append(dbf.get_values_of_list(row, col_index))
                                                data_result[row[col_index[4]]] = [row[col_index[0]], row[col_index[1]],
                                                                                  row[col_index[2]], row[col_index[3]],
                                                                                  row[col_index[5]]]
                                            break
                                        else:
                                            try:
                                                if ii == 0:
                                                    time_list.append(float(standard_time_dict[
                                                                               interval + '_' + interval_min + '_' + str(
                                                                                   float(row[col_index[3]]))]))
                                                    ii += 1
                                                else:
                                                    ii += 1
                                                    time_list.append(float(standard_time_dict[
                                                                               interval_new + '_' + interval_min + '_' +
                                                                               str(float(row[col_index[3]]))]))
                                            except:
                                                time_list.append(5.0)
                                            interval_new = interval_min
                                            try:
                                                gap = set(standard_dict[interval_new]).intersection(set(interval_list))
                                            except:
                                                break
                                            if gap:
                                                interval_min = gap.pop()
                                            else:
                                                break
                                            if ii > 30:
                                                break
                    else:
                        # ʱ���ת��
                        try:
                            time_ls = data_time[-4]
                        except:
                            continue
                        if '5' > data_time[-4] >= '0':
                            data_time = data_time[:-4] + '0:00'
                        else:
                            data_time = data_time[:-4] + '5:00'
                        # ������������
                        if treat_type == 'statistic':
                            try:
                                data_result[
                                    row[col_index[0]][:16] + '_' + data_time + '_' + str(float(row[col_index[3]]))] += 1
                            except:
                                data_result[
                                    row[col_index[0]][:16] + '_' + data_time + '_' + str(float(row[col_index[3]]))] = 1
                        else:
                            row[col_index[1]] = data_time
                            # data_result.append(dbf.get_values_of_list(row, col_index))
                            data_result[row[col_index[4]]] = [row[col_index[0]], row[col_index[1]], row[col_index[2]],
                                                              row[col_index[3]], row[col_index[5]]]

    if if_return:
        return data_result
    else:
        # ���ֵ����͵��������ݣ�ת��Ϊ�������ͣ������б���
        data_result_list = []  # ������żܵ�����
        for key in data_result.keys():
            key_list = key.split('_')
            key_list.append(data_result[key])
            data_result_list.append(key_list)

        with open('./4.data_check/cangestion/total_gantry_flow_num_data_202205.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(data_result_list)


'''
    ����ʱ�䣺2022/8/9
    ���ʱ�䣺2022/8/9
    ���ܣ���֤�ż�����������������������żֱܷ����ÿ�������ĶԱȣ����żܸ�ʱ������ǰ��ĶԱ�
    �޸�ʱ�䣺
'''


def chect_the_flow_data():
    """
    ��֤�ż�����������������������żֱܷ����ÿ�������ĶԱȣ����żܸ�ʱ������ǰ��ĶԱ�
    :return:
    """
    # �õ������ڵ��ļ���ַ
    paths = dop.path_of_holder_document('./1.Gantry_Data/202207/gantry_path/', True)
    # �������żܵ����鼯
    gantrys = ["G003061003000120", "G003061003000210", "G003061003000110", "G003061003000310", "G003061003000220",
               "G003061003000410", "G003061003000320", "G003061003000510", "G003061003000420", "G003061003000520",
               "G003061003000710", "G003061003000720", "G003061003000810", "G003061003000820", "G003061003001310",
               "G003061003000610", "G003061003000620", "G003061003000910", "G003061003000920", "G003061003001010",
               "G003061003001020", "G003061003001120", "G003061003001110", "G003061003001220", "G003061003001210",
               "G003061003001320"]
    data_revise_dict = {}
    data_result = []
    for path in paths:
        if '20220723' >= path[-12:-4] or path[-12:-4] >= '20220730':
            continue
        # ��ȡ�����շѵ�Ԫ������ĸ��ż�����
        # for gantry in gantrys:
        col_name = ['TOLLINTERVALID' for i in range(16)]
        data_interval = dbf.get_result_from_data_with_colValue([path], col_name, gantrys, 'in')
        # ��ȡ������Ľ������
        data_revise = get_real_flow_with_time_revise([path], if_return=True)
        for key in data_revise.keys():
            key_gantry = key.split('_')
            if key_gantry[0] in gantrys:
                try:
                    data_revise_dict[key_gantry[0]] += data_revise[key]
                except:
                    data_revise_dict[key_gantry[0]] = data_revise[key]
        for i, gantry in enumerate(gantrys):
            try:
                data_result.append([path[-12:-4], gantry, data_interval[i], data_revise_dict[gantry]])
            except:
                data_result.append([path[-12:-4], gantry, data_interval[i], 0])

    with open('./4.data_check/cangestion/check_flow_num.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data_result)


'''
    ����ʱ�䣺2022/8/10
    ���ʱ�䣺2022/8/10
    ���ܣ�ͳ���������żܸ�ʱ������ǰ�������
    �޸�ʱ�䣺
'''


def check_gantry_flow_data_after_revise():
    # �õ������ڵ��ļ���ַ

    # paths = dop.path_of_holder_document('./1.Gantry_Data/20220/gantry_path/', True)
    paths = dop.path_of_holder_document('./1.Gantry_Data/202206/gantry_path/', True)
    paths.extend(dop.path_of_holder_document('./1.Gantry_Data/202205/gantry_path/', True))
    paths.extend(dop.path_of_holder_document('./1.Gantry_Data/202204/gantry_path/', True))
    paths.extend(dop.path_of_holder_document('./1.Gantry_Data/202203/gantry_path/', True))
    paths.extend(dop.path_of_holder_document('./1.Gantry_Data/202202/gantry_path/', True))
    paths.extend(dop.path_of_holder_document('./1.Gantry_Data/202201/gantry_path/', True))
    paths.extend(dop.path_of_holder_document('./1.Gantry_Data/202112/gantry_path/', True))
    paths.extend(dop.path_of_holder_document('./1.Gantry_Data/202111/gantry_path/', True))
    paths.extend(dop.path_of_holder_document('./1.Gantry_Data/202110/gantry_path/', True))
    paths.extend(dop.path_of_holder_document('./1.Gantry_Data/202109/gantry_path/', True))
    paths.extend(dop.path_of_holder_document('./1.Gantry_Data/202108/gantry_path/', True))
    paths.extend(dop.path_of_holder_document('./1.Gantry_Data/202107/gantry_path/', True))
    middle_path = './2.Middle_Data/202207/main_middledata_'
    gantrys = ["G003061003000120", "G003061003000210", "G003061003000110", "G003061003000310", "G003061003000220",
               "G003061003000410", "G003061003000320", "G003061003000510", "G003061003000420", "G003061003000520",
               "G003061003000710", "G003061003000720", "G003061003000610", "G003061003000620", "G003061003000910",
               "G003061003000920", "G003061003000810", "G003061003000820", "G003061003001310", "G003061003001210",
               "G003061003001320", "G003061003001010", "G003061003001020", "G003061003001110", "G003061003001120",
               "G003061003001410", "G003061003001420", "G003061003001220"]
    path_list = []
    gantry_path_list = []
    for path in paths:
        if '20220730' <= path[-12:-4]:
            path_list.append(path)
            gantry_path_list.append(middle_path + path[-12:-4] + '.csv')
    # ��ȡ��������żܸ�ʱ�ε�����
    data_revise = get_real_flow_with_time_revise(path_list, if_return=True)
    # ��ȡԭʼ���żܸ�ʱ�ε�����
    gantry_flow = get_data_of_two_gantry_have(gantry_path_list, gantrys, 'or', 'dict')

    data_result = []
    # ���и��żܸ�ʱ�θ����͵ĶԱ�
    for key in data_revise.keys():
        key_list = key.split('_')
        if key_list[0] in gantrys:
            key_list.append(data_revise[key])
            try:
                key_list.append(gantry_flow[key])
            except:
                key_list.append(0)
            data_result.append(key_list)

    with open('./4.data_check/cangestion/flow_data_after_revise.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data_result)


'''
    ����ʱ�䣺2022/9/1
    ���ʱ�䣺2022/9/1
    ���ܣ����ݸ�·�θ����͵��ٶȽ���ӵ���ж�
    �ؼ��ʣ������ٶȣ�ӵ���ж�
    �޸�ʱ�䣺
'''


def charge_congestion_with_speed(data_speed, time_point, data_flow, threshold_data, target='XB', threshold=1,
                                 gantrys=None):
    """
    ���ݸ�·�θ����͵��ٶȽ���ӵ���ж�
    :param gantrys:
    :param threshold: �ж�Ϊӵ�µ���ֵ��Ĭ��Ϊ100%����ȫ���ٶȵ�����ֵ���ж�Ϊӵ��
    :param data_flow: ʵʱ��ȡ�ĵ�ǰʱ�θ�·�θ����͵��������ݣ��ֵ�����
    :param target: �����ж���Ŀ��·�Σ�Ĭ��Ϊ����
    :param data_speed: ʵʱ��ȡ�ĵ�ǰʱ�θ�·�θ����͵��ٶ����ݣ��ֵ�����
    :param threshold_data: ��·�θ����͵��ٶ���ֵ���ֵ�����
    :return: �����շѵ�ԪId��Ӧ�Ƿ�ӵ�µ��ֵ�����
    """
    # �ж������Щ·��
    if len(gantrys) == 0:
        if target == 'XB':
            # ��ȡҪ�����ж����շѵ�Ԫ����
            gantrys = kp.get_parameter_with_keyword('XB_gantrys')
    else:
        pass
    # ��ȡ���г��͵ļ���
    vehicle_type = kp.get_parameter_with_keyword('vehicle_type')

    # ���ڼ�¼��·��ӵ��״̬
    congestion_result = {}
    # ���������������շѵ�ԪID
    for gantry in gantrys:
        # if gantry == 'G003061003000820':
        #     print(1)
        treat_num = 0  # ���������ֵ�ĳ�����
        total_num = 0  # ��������ж����г�����
        # �������еĳ���
        for i, v_type in enumerate(vehicle_type):
            # ��ÿ����Ԫ�����г��ͳ��ٽ�����ֵ�Ա�
            # ����޸ó������ݣ�ֱ������
            try:
                this_flow = float(data_flow[gantry][i])
            except:
                continue
            # ��������ͳ���������ͬʱ����Ϊ0ʱ���򲻲����ж�
            a = data_speed[gantry + '_' + time_point][i]
            if this_flow < 5 or data_speed[gantry + '_' + time_point][i] == 0:
                continue
            else:
                # �ж��Ƿ������ֵ
                try:
                    treshold_num = float(threshold_data[gantry + '_' + str(float(v_type))])
                except:
                    treshold_num = 30
                if float(data_speed[gantry + '_' + time_point][i]) <= treshold_num:
                    treat_num += this_flow
                else:
                    pass
                total_num += this_flow
        # �������ֵ������ռ�ȴﵽ��ֵ����·�μ�¼ӵ��
        if total_num != 0 and (treat_num / total_num) >= threshold:
            congestion_result[gantry] = 1
        else:
            congestion_result[gantry] = 0

    return congestion_result


'''
    ����ʱ�䣺2022/9/6
    ���ʱ�䣺2022/9/6
    ���ܣ����ݸ�·��ǰ�������仯���߽���ӵ���ж�
    �ؼ��ʣ����������仯���������ƶȶԱȣ�ӵ���ж�
    �޸�ʱ�䣺
'''


def charge_congestion_with_flow_curve(old_inout_data_in, old_inout_data_out, new_inout_data, gantry_back_data,
                                      target='XB', gantrys=None):
    """
    ���ݸ�·��ǰ�������仯���߽���ӵ���ж�
    :param gantrys:
    :param target:
    :param old_inout_data_in:
    :param old_inout_data_out:
    :param dict new_inout_data:  ���żܵ�ǰʱ�ε�����������������ֵ�
    :param dict gantry_back_data: ���żܶ�Ӧ��һ�żܵĹ�ϵ�ֵ�
    :return:
    """
    # ���ڼ�¼��·��ӵ��״̬
    congestion_result = {}
    if len(gantrys) == 0:
        if target == 'XB':
            # ��ȡҪ�����ж����շѵ�Ԫ����
            gantrys = kp.get_parameter_with_keyword('XB_gantrys')
    else:
        pass
    # �����ֵ������е��ż�ID������ǰ���ż������仯�Ա�
    for gantry in gantrys:
        gantry_in = gantry + '_in'
        gantry_out = gantry + '_out'
        # ��õ�ǰ�żܵĸ�ʱ������
        try:
            this_gantry_out = old_inout_data_out[gantry][1:]
        except:
            congestion_result[gantry] = 0
            continue
        try:
            this_gantry_out.append(new_inout_data[gantry_out])
        except:
            this_gantry_out.append(0)
        # �����һ���żܵĸ�ʱ������
        try:
            this_gantry_in = old_inout_data_in[gantry]
        except:
            # ���û����һ���żܣ���ʾ���ż���Ϣ��������һ���ż�
            print(gantry_in)
            continue
        # ��ǰ���ż��������ݴ��뺯�������������ݵ����ƶ�
        dist = dbf.compute_distance_of_two_list('DTW', this_gantry_out, this_gantry_in,
                                                {'dist': 'abs', 'warp': 1, 'w': 2, 's': 0.6})
        # ������ֵ���ж����ƶ��Ƿ��쳣
        speed_threshold_data = dbf.get_disc_from_document('./statistic_data/basic_data/DTW_threshold_data.csv',
                                                          ['id', 'num'], encoding='utf-8', key_for_N=False)
        dist_threshold = float(speed_threshold_data[gantry])  # bug:�˴���ֵ��ʱ�趨Ϊ3��������Ҫͳ�ƺ������ƶ���ֵ��Χ
        if dist_threshold < 10:
            dist_threshold = 40
        if dist > dist_threshold:  # ���������ֵ����ֵΪ1��������ӵ�����
            congestion_result[gantry] = 1
        else:  # ���δ����ֵ����ֵΪ0��������ӵ�����
            congestion_result[gantry] = 0

    return congestion_result


'''
    ����ʱ�䣺2022/9/1
    ���ʱ�䣺2022/9/1
    ���ܣ��ж�·��ӵ�µĵȼ�(���ݳ����ĳ���������·�γ����ж�)
    �ؼ��ʣ�ӵ�µȼ�
    �޸�ʱ�䣺
'''


def charge_congestion_level_by_length(have_data, charge_gantry, distance_data):
    """
    �ж�·��ӵ�µĵȼ�(���ݳ����ĳ��������ж�)
    :param dict distance_data: ���żܼ�ĳ����ֵ�����
    :param dict have_data: ���żܼ�ĳ���������
    :param list charge_gantry: ��Ҫ����ӵ�µȼ��ж����żܼ���
    :return:
    """
    level = {}  # ���ڱ���ÿ��·�ε�ӵ�µȼ�
    # ������Ҫ���������·��
    for gantry in charge_gantry:
        # ��ȡ��·�θ�ʱ�̵�·������
        have_num = have_data[gantry]
        # ��ȡ��·�γ���
        distance = distance_data[gantry]
        # ��ȡ��·�ε�·��, bug:��·���̶�Ϊ4
        lane_num = 4
        # ����ÿ��·��ӵ��ϵ��
        rate = have_num / (distance * lane_num)
        # ת��Ϊӵ�µȼ�
        if rate > 1:
            level[gantry] = 5
        elif 1 >= rate > 0.5:
            level[gantry] = 4
        elif 0.5 >= rate > 0.1:
            level[gantry] = 3
        elif 0.1 > rate > 0.01:
            level[gantry] = 2
        else:
            level[gantry] = 1
    return level


'''
    ����ʱ�䣺2022/9/1
    ���ʱ�䣺2022/9/1
    ���ܣ�����ÿ��·�εĵ�·������
    �ؼ��ʣ�������
    �޸�ʱ�䣺No.1 2022/9/30,�޸����������������Ϊ�����͸�·�����������صĳ�����Ҳ���ܳ��������Ϊ���������ͳ�����
            No.2 2023/01/28,�޸�����ע��Σ����շ�վ����������վ������Ԥ������滻Ϊ���ݿ��ȡ
'''


def compute_num_of_gantry(flow_data, now_station_flow_vType_dict, last_time_num, gantry_back_data, last_station_dict,
                          this_time, province_gantrys, gantrys=None):
    """
    ����ÿ��·�εĵ�·������
    :param province_gantrys: ��ʡʡ���ż�ID
    :param gantrys: �漰���������ż�ID
    :param now_station_flow_vType_dict: ʵʱ������շ�վ��ǰʱ�̸�������ʻ��ʻ������
    :param flow_out_station_data: ��ʷ���ݸ�ʱ�θ��շ�վ�������ϵ�ʻ������
    :param list flow_in_station_data: ��ʷ���ݸ�ʱ�θ��շ�վ�������ϵ�ʻ������
    :param bool if_history: �ж��������ʷ���ݻ������ʵʱ���ݽ��д���
    :param dict last_time_num: ��·����һ��ʱ�̸����͵ĳ�����
    :param str this_time: ��ʱ�ε�ʱ����Ϣ
    :param dict last_station_dict: �շѵ�Ԫ����һ���շ�վ��Ӧ��ϵ�ֵ�
    :param dict gantry_back_data: �շѵ�Ԫ����һ����Ԫ�Ķ�Ӧ��ϵ�ֵ�
    :param dict flow_data: ��·�ε�ǰʱ�ε���������
    :return:
    """
    # �����͸�ֵ
    vehicle_types = [['1'], ['2'], ['3'], ['4'], ['11'], ['12'], ['13'], ['14'], ['15'], ['16']]
    # ��ȡҪ���м���������շѵ�ԪID
    # if target == 'XB':
    #     gantrys = kp.get_parameter_with_keyword('XB_gantrys')
    # ���������շѵ�Ԫ���ֱ�����������������ͳ�����
    gantry_inout_data = {}  # ���ڱ������е�Ԫ��������������
    gantry_have_data = {}  # ���ڱ������е�Ԫ�ĳ�����
    gantry_have_total_data = {}  # ���ڱ������е�Ԫ���ܳ�����
    gantry_inout_total_data = {}  # ���ڱ������е�Ԫ������������
    for gantry in gantrys:
        in_num = {}  # ������ż��¸����͵�����
        out_num = {}  # ������ż��¸����͵�����
        this_time_num = {}  # �����·���¸����͵ĳ�����
        # 1.����������
        # 1)��ȡ��һ���żܵ�ID
        try:
            last_gantrys = gantry_back_data[gantry]
        except:
            continue
        last_gantry = ''
        if len(last_gantrys) > 2:
            continue
        elif len(last_gantrys) == 1:
            pass
        else:
            for i in range(len(last_gantrys)):
                if len(gantry_back_data[last_gantrys[i]]) == 1 and last_gantrys[i] not in province_gantrys:
                    pass
                elif last_gantrys[i] not in gantrys:
                    continue
                else:
                    last_gantry = last_gantrys[i]
            if last_gantry == '':
                continue

        # 2)��ȡ��һ���շ�վ����վ���� change:2023/01/28
        # 2.1)��ȡ��һ���շ�վID
        last_station = last_station_dict[gantry]
        if last_station == '':
            continue
        # # 2.2)��������ʷ���ݽ��г���������
        # if if_history:
        #     station_in_dict = {}
        #     for vType in vehicle_types:
        #         try:
        #             station_in_dict[vType[0]] = flow_in_station_data[
        #                 last_station + '_' + gantry[:-2] + '_' + this_time + '_' + vType[0]]
        #         except:
        #             station_in_dict[vType[0]] = 0
        # else:
        #     station_in_dict = now_station_flow_vType_dict  # changed by 2023/01/28

        # 3)����������
        for i, vType in enumerate(vehicle_types):
            try:
                last_gantry_num = float(flow_data[last_gantry][i])  # changed at 2023/01/28
            except:
                last_gantry_num = 0
            try:
                # changed at 2023/01/30
                last_station_num = float(now_station_flow_vType_dict[last_station + '_1_' + gantry][i])
            except:
                last_station_num = 0
            try:
                # changed at 2023/01/30
                last_station_num_maybe = int(float(now_station_flow_vType_dict[last_station + '_1_'][i]) / 2)
            except:
                last_station_num_maybe = 0
            in_num[vType[0]] = last_gantry_num + last_station_num + last_station_num_maybe

        # 2.����������
        # 1)��ȡ��һ���շ�վ��Ԥ����վ����
        # 1.1)��������ʷ���ݽ��г���������
        # if last_gantry == '':
        #     station_out_dict = {}
        #     for vType in vehicle_types:
        #         station_out_dict[vType[0]] = 0
        # else:
        #     if if_history:
        #         station_out_dict = {}
        #         for vType in vehicle_types:
        #             try:
        #                 station_out_dict[vType[0]] = flow_out_station_data[
        #                     last_station + '_' + last_gantry[:-2] + '_' + this_time + '_' + vType[0]]
        #             except:
        #                 station_out_dict[vType[0]] = 0
        #     else:
        #         station_out_dict = now_station_flow_vType_dict  # changed by 2023/01/28

        # 1)��ȡ���żܵ�����
        for i, vType in enumerate(vehicle_types):
            # 1.1)��ȡ���żܵ�����
            try:
                last_gantry_num = float(flow_data[gantry][i])  # changed at 2023/01/28
            except:
                last_gantry_num = 0
            # 1.2)��ȡ��һ���շ�վ����վ����
            try:
                # changed by 2023/01/30
                last_station_num = float(now_station_flow_vType_dict[last_station + '_2_' + last_gantry][i])
            except:
                last_station_num = 0
            # 1.3)��ȡ��һ���շ�վ�Ĳ�����վ����
            try:
                # changed by 2023/01/30
                if last_gantry != '':
                    last_station_num_maybe = int(float(now_station_flow_vType_dict[last_station + '_2_'][i]) / 2)
                else:
                    last_station_num_maybe = 0
            except:
                last_station_num_maybe = 0
            # 1.4)������������
            out_num[vType[0]] = last_gantry_num + last_station_num + last_station_num_maybe
            # 1.5)��ȡ֮ǰ��·��֮ǰ�ĳ���������
            try:
                last_num = float(last_time_num[gantry + '_' + vType[0]])
            except:
                last_num = 0
            # 1.6)���и�·�εĸ����ͳ���������
            # gantry_have_data[gantry + '_' + vType[0]] = last_num + in_num[vType[0]] - out_num[vType[0]]
            try:
                gantry_have_data[gantry + '_' + this_time].append(last_num + in_num[vType[0]] - out_num[vType[0]])
            except:
                gantry_have_data[gantry + '_' + this_time] = [last_num + in_num[vType[0]] - out_num[vType[0]]]
            # 1.7)�����·�ε��ܳ�����
            try:
                gantry_have_total_data[gantry] += last_num + in_num[vType[0]] - out_num[vType[0]]
            except:
                gantry_have_total_data[gantry] = last_num + in_num[vType[0]] - out_num[vType[0]]
            # 1.8)���и�·�θ������������ļ���
            try:
                gantry_inout_data[gantry + '_in'].append(in_num[vType[0]])
            except:
                gantry_inout_data[gantry + '_in'] = [in_num[vType[0]]]
            # 1.9)���и�·�θ�����������ļ���
            try:
                gantry_inout_data[gantry + '_out'].append(out_num[vType[0]])
            except:
                gantry_inout_data[gantry + '_out'] = [out_num[vType[0]]]
            # 1.10)���и�·�����������ļ���
            try:
                gantry_inout_total_data[gantry + '_in'] += in_num[vType[0]]
            except:
                gantry_inout_total_data[gantry + '_in'] = in_num[vType[0]]
            # 1.11)���и�·����������ļ���
            try:
                gantry_inout_total_data[gantry + '_out'] += out_num[vType[0]]
            except:
                gantry_inout_total_data[gantry + '_out'] = out_num[vType[0]]

    return gantry_have_data, gantry_inout_data, gantry_have_total_data, gantry_inout_total_data


'''
    ����ʱ�䣺2022/9/9
    ���ʱ�䣺2022/9/9
    ���ܣ�����������ʷ���żܵ������������DTWϵ��
    �ؼ��ʣ��������������DTWϵ������������
    �޸�ʱ�䣺
'''


def compute_DTW_of_gantrys(start_time, end_time):
    """
    ����������ʷ���żܵ������������DTWϵ��
    :param start_time:
    :param end_time:
    :return:
    """
    start_time = start_time + ' 00:00:00'  # ��ȡ��ʼʱ��
    end_time = end_time + ' 00:00:00'  # ��ȡ��ֹʱ��
    # ��ȡ��ʷ����������ݣ������䵽���ż���
    in_data_dict = {}  # ��Ÿ��żܵ�������������
    out_data_dict = {}  # ��Ÿ��żܵ������������
    have_num_dict = {}  # ��Ÿ��żܵĳ���������
    time_data_dict = {}  # ��Ÿ��żܵļ�¼ʱ������
    with open('./4.statistic_data/basic_data/820_have.csv') as f:
        for i, row in enumerate(f):
            row = row.split(',')
            row[-1] = row[-1][:-1]
            if i == 0:
                row[0] = row[0][1:]
            if row[1] < start_time or row[1] > end_time:
                continue
            # ���ڸ�����û���ֶ�����ֱ������
            try:
                in_data_dict[row[0]].append(float(row[2]))
            except:
                in_data_dict[row[0]] = [float(row[2])]
            try:
                out_data_dict[row[0]].append(float(row[3]))
            except:
                out_data_dict[row[0]] = [float(row[3])]
            try:
                have_num_dict[row[0]].append(float(row[4]))
            except:
                have_num_dict[row[0]] = [float(row[4])]
            try:
                time_data_dict[row[0]].append(row[1])
            except:
                time_data_dict[row[0]] = [row[1]]
    # ����������������ȸ���ʱ���������
    # �������е��ż�ID������DTWϵ������
    result_data = []  # ���ڱ�����żܸ�ʱ������������ƶȵ�����
    for key in in_data_dict.keys():
        compute_ls1 = []  # ���ÿ��ʱ����ʱ�ļ������ݡ�����������
        compute_ls2 = []  # ���ÿ��ʱ����ʱ�ļ������ݡ����������
        # �������żܵ���������������ݣ�����DTW
        for i in range(len(in_data_dict[key])):
            if i == 0:
                compute_ls1.append(in_data_dict[key][i])
            else:
                compute_ls1.append(in_data_dict[key][i])
                compute_ls2.append(out_data_dict[key][i])
            # �ж�������������Ƿ�ﵽ��13��������ﵽ�ˣ���������ƶȼ���
            if len(compute_ls2) < 13:
                continue
            else:
                # ����DTWģ�͵�������
                dist = dbf.compute_distance_of_two_list('DTW', compute_ls1[:-1], compute_ls2,
                                                        {'dist': 'abs', 'warp': 1, 'w': 2, 's': 0.6})
                sum_value = sum([compute_ls1[i - 3] - compute_ls2[i - 3] for i in range(3)])
                result_data.append([key, time_data_dict[key][i], have_num_dict[key][i], in_data_dict[key][i],
                                    out_data_dict[key][i], dist, sum_value])
                compute_ls1.pop(0)
                compute_ls2.pop(0)
    # �洢���ż����ƶȽ������
    save_name = './4.statistic_data/basic_data/'
    with open(save_name + 'DTW_num_data_820.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(result_data)


'''
    ����ʱ�䣺2022/9/9
    ���ʱ�䣺2022/9/9
    ���ܣ��Ը��żܵ�DTW���ϵ�����зֲ���ϣ�����ù涨���Ŷ�֮�����ֵDTWϵ��
    �ؼ��ʣ��ֲ���ϡ�DTWϵ����ֵ
    �޸�ʱ�䣺
'''


def get_DTW_threshold_of_gantrys(threshold):
    """
    �Ը��żܵ�DTW���ϵ�����зֲ���ϣ�����ù涨���Ŷ�֮�����ֵDTWϵ��
    :param float threshold: ӵ�·����ĸ���
    :return:
    """
    # ��ȡÿ���żܵ�DTWϵ��
    DTW_dict = {}  # ��Ÿ��żܵļ�¼ʱ������
    with open('./4.statistic_data/basic_data/DTW_num_data.csv') as f:
        for i, row in enumerate(f):
            row = row.split(',')
            row[-1] = row[-1][:-1]
            # ���ڸ�����û���ֶ�����ֱ������
            if float(row[6]) < 0:
                continue
            try:
                DTW_dict[row[0]].append(round(float(row[5]), 1))
            except:
                DTW_dict[row[0]] = [round(float(row[5]), 1)]
    # ����ÿ���żܣ�����ÿ���ż�DTWϵ������ֵ
    result = []  # ����DTW��ֵ���
    for key in DTW_dict.keys():
        print(key)
        start_value, end_value, x_value, y_value = dbf.get_fitter(DTW_dict[key], threshold, (1 - threshold))
        result.append([key, end_value])
    # ����ÿ���żܵ�DTW��ֵ
    save_name = './4.statistic_data/basic_data/'
    with open(save_name + 'DTW_threshold_data.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(result)


'''
    ����ʱ�䣺2022/9/9
    ���ʱ�䣺2022/9/9
    ���ܣ�����DTW��ֵ��ȡ���żܳ���DTW�ĳ�������ʱ���
    �ؼ��ʣ���������ֵ��DTW��ֵ
    �޸�ʱ�䣺
'''


def get_have_threshold_of_gantrys():
    """"""
    # ��ȡ���żܵ�DTW��ֵ
    DTW_threshold_dict = dbf.get_disc_from_document('./4.statistic_data/basic_data/DTW_threshold_data.csv',
                                                    [0, 1], encoding='gbk', key_for_N=False, ifIndex=False)
    # �������г��������ݣ��ֱ�ÿ���żܳ�����DTW��ֵ�ĳ�������¼����
    result = []  # ���ڼ�¼����DTW��ֵ���żܺͳ�������Ϣ
    with open('./4.statistic_data/basic_data/DTW_num_data.csv') as f:
        for i, row in enumerate(f):
            row = row.split(',')
            row[-1] = row[-1][:-1]
            if float(row[5]) > float(DTW_threshold_dict[row[0]]) and float(row[6]) > 2:
                result.append([row[0], row[1], row[2]])
    # ����
    save_name = './4.statistic_data/basic_data/'
    with open(save_name + 'have_threshold_data.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(result)


def compute_XB_gantry_num():
    """

    :return:
    """
    num = 0
    with open('./Data_Origin/tollinterval.csv') as f:
        for j, row in enumerate(f):
            row = row.split(',')
            if j == 0:
                index = dbf.get_indexs_of_list(row, ['id'])
            if row[index[0]] == 'G0030610030':
                num += 1
    return num


'''
    ����ʱ�䣺2022/9/12
    ���ʱ�䣺2022/9/14
    ���ܣ����������żܵ�ǰ����������ֵ�ĶԱȣ����������ֵͬʱ���Сʱ����Գ�����ֵ���ж�ΪԤ��ӵ��
    �ؼ��ʣ���������ֵ��ӵ��Ԥ�У���Сʱ����Ԥ��
    �޸�ʱ�䣺
'''


def charge_congestion_by_haveNum(now_have_num, this_time, basic_data, station_flow_history_dict_in,
                                 station_flow_history_dict_out, gantry_back_data,
                                 last_station_dict, province_gantrys, gantrys=[]):
    """
    ���������żܵ�ǰ����������ֵ�ĶԱȣ����������ֵͬʱ���Сʱ����Գ�����ֵ���ж�ΪԤ��ӵ��
    :param station_flow_history_dict_in:
    :param list basic_data: ������ǰʱ�̼�֮ǰ48��ʱ�̵���������
    :param str this_time: ��ǰʱ��
    :param dict now_have_num: ��ǰ���żܳ������ֵ�
    :return:
    """
    congestion_charge = {}  # ������żܵ��Ƿ�ӵ�½��������0Ϊ��ӵ�£�1ΪԤ��ӵ�£�2Ϊͨ�л���
    # ��ȡ���żܵĳ�����������ֵ
    have_threshold_dict = dbf.get_disc_from_document('./statistic_data/basic_data/have_threshold_data.csv',
                                                     [0, 1], encoding='gbk', key_for_N=False, ifIndex=False,
                                                     ifNoCol=True)
    if len(gantrys) == 0:
        gantrys = kp.get_parameter_with_keyword('XB_gantrys')
    # �Աȸ��żܵĵ�ǰ�������ͷ�����ֵ
    out_data = []  # ���ڱ��泬����ֵ���ż�ID
    for gantry in gantrys:
        try:
            now_have = now_have_num[gantry]
        except:
            now_have = 0
        try:
            threshold_have = have_threshold_dict[gantry]
        except:
            threshold_have = 400
        if now_have >= (float(threshold_have) * 1.5):
            congestion_charge[gantry] = 2
        elif now_have >= float(threshold_have):
            out_data.append(gantry)
        else:
            congestion_charge[gantry] = 0

    # �������г�����ֵ���ż�ID��Ԥ����δ����Сʱ�������������������������ֵ��50%���ж�ΪԤ��ӵ�£�bug��ӵ��Ԥ�еİٷֱȹ̶�Ϊ��20%
    for i in range(len(out_data)):
        last_gantrys = gantry_back_data[out_data[i]]
        for j in range(len(last_gantrys)):
            if len(gantry_back_data[last_gantrys[j]]) == 1 and last_gantrys[i] not in province_gantrys:
                pass
            elif last_gantrys[j] not in gantrys:
                continue
            else:
                last_gantry = last_gantrys[j]
        # ��ȡδ����Сʱ��һ���żܵ�����Ԥ��
        last_gantry_flow = compute_future_flow_of_gantry_station(6, last_gantry, this_time,
                                                                 basic_data, 'gantry')
        # ��ȡδ����Сʱ��ǰ�żܵ�����Ԥ��
        this_gantry_flow = compute_future_flow_of_gantry_station(6, out_data[i], this_time,
                                                                 basic_data, 'gantry')
        # ��ȡδ����Сʱ�շ�վ���������Ԥ��
        station_in_flow = compute_future_flow_of_gantry_station(6,
                                                                last_station_dict[out_data[i]] + '_' + out_data[i][:-2],
                                                                this_time, station_flow_history_dict_in, 'station_in')
        # ��ȡδ����Сʱ�շ�վ���������Ԥ��
        station_out_flow = compute_future_flow_of_gantry_station(6,
                                                                 last_station_dict[out_data[i]] + '_' +
                                                                 last_gantry[:-2],
                                                                 this_time, station_flow_history_dict_out, 'station_out')
        # ����δ�����Сʱ��ĳ�����
        try:
            future_have_num = now_have_num[out_data[i]] + dbf.compute_dict_by_group(last_gantry_flow, [], 'total_sum',
                                                                                    '') + dbf.compute_dict_by_group(
                station_in_flow, [], 'total_sum', '') - \
                              dbf.compute_dict_by_group(this_gantry_flow, [], 'total_sum',
                                                        '') - dbf.compute_dict_by_group(station_out_flow, [],
                                                                                        'total_sum', '')
        except:
            future_have_num = 0

        # �ж��Ƿ񳬳���ֵ
        if future_have_num >= (float(have_threshold_dict[out_data[i]]) * 1.5):
            congestion_charge[out_data[i]] = 1
        else:
            congestion_charge[out_data[i]] = 0

    # �����ж����
    return congestion_charge


def get_feature_of_flow_calculate(path, treat_type='gantry', if_data=False):
    """
    ��ȡ�����żܸ�ʱ�ε�����ѵ����������
    :param treat_type: ����������ͣ�gantryΪ�żܣ�stationΪ�շ�վ
    :param if_data: �ж��Ƿ���������ݵĴ���FALSE��ʾ�ǶԵ�ַ�ļ����д���TRUE��ʾ�����pathΪ�ؼ���������
    :param path: ��������żܸ������������ݵĵ�ַ
    :return:
    """
    # ��������ʱ���ֵ�
    holiday = {'20210919': [3, 1], '20210920': [3, 2], '20210921': [3, 3], '20211001': [7, 1], '20211002': [7, 2],
               '20211003': [7, 2], '20211004': [7, 2], '20211005': [7, 2], '20211006': [7, 2], '20211007': [7, 3],
               '20220101': [3, 1], '20220102': [3, 2], '20220103': [3, 3], '20220131': [7, 1], '20220201': [7, 2],
               '20220202': [7, 2], '20220203': [7, 2], '20220204': [7, 2], '20220205': [7, 2], '20220206': [7, 2],
               '20220403': [3, 1], '20220404': [3, 2], '20220405': [3, 3], '20220501': [5, 2], '20220502': [5, 2],
               '20220503': [5, 2], '20220504': [5, 3], '20220430': [5, 1], '20220603': [3, 1], '20220604': [3, 2],
               '20220605': [3, 3], '20220910': [3, 1], '20220911': [3, 2], '20220912': [3, 3]}
    if if_data:  # ��������Ĳ���������ģ�Ͳ�������
        data_sample_ls = []
        # �жϵ�ǰ�����Ƿ�Ϊ�ڼ���,�Լ����������͵�ǰ�������ڽ׶�
        try:
            holiday_list = holiday[path[:10].replace('-', '')]
            data_sample_ls.append(1)
            data_sample_ls.extend(holiday_list)
        except:
            data_sample_ls.extend([0, 0, 0])
        path = datetime.datetime.strptime(path, '%Y-%m-%d %H:%M:%S')
        # �ж��Ƿ��ǹ�����
        if is_workday(path):
            data_sample_ls.append(1)
        else:
            data_sample_ls.append(0)
        # �ж������ڼ�
        day = datetime.date(path.year, path.month, path.day)
        data_sample_ls.append(day.isoweekday())
        # ���㵱ǰʱ�κ�
        minute = path.hour * 60 + path.minute + path.second / 60
        if minute == 0:
            data_sample_ls.append(1)
        else:
            data_sample_ls.append(math.ceil(minute / 5))
        # add month
        data_sample_ls.append(path.month)

        return data_sample_ls
    else:  # ����ļ�����������������
        if treat_type == 'gantry':
            # �õ��żܳ��������Ķ�Ӧ�ֵ�
            data = dbf.get_disc_from_document(path, [0, 1, 2, 3], encoding='utf-8', key_for_N=False, key_length=3,
                                              ifIndex=False, key_for_N_type='list', sign='_')
            # �������żܳ��ͣ�����ÿ����¼��ѵ������
            data_sample = []  # ���ڱ���ѵ������
            for key in data.keys():
                data_sample_ls = []  # ���ڱ��浥��һ��������
                key_list = key.split('_')
                if key_list[1] <= '2021-08-01 04:00:00':
                    continue
                data_sample_ls.extend(key_list)
                # ��ǰ4��Сʱ�������������ݻ�ȡ��
                this_time = datetime.datetime.strptime(key_list[1], '%Y-%m-%d %H:%M:%S')
                for i in range(48):
                    new_time = this_time + datetime.timedelta(minutes=(5 * (i + 1) * (-1)))
                    new_key = key_list[0] + '_' + datetime.datetime.strftime(new_time, '%Y-%m-%d %H:%M:%S') + '_' + \
                              key_list[2]
                    try:
                        new_flow = data[new_key]
                        data_sample_ls.append(new_flow)
                    except:
                        data_sample_ls.append(0)
                # �жϵ�ǰ�����Ƿ�Ϊ�ڼ���,�Լ����������͵�ǰ�������ڽ׶�
                try:
                    holiday_list = holiday[key_list[1][:10].replace('-', '')]
                    data_sample_ls.append(1)
                    data_sample_ls.extend(holiday_list)
                except:
                    data_sample_ls.extend([0, 0, 0])
                # �ж��Ƿ��ǹ�����
                if is_workday(this_time):
                    data_sample_ls.append(1)
                else:
                    data_sample_ls.append(0)
                # �ж������ڼ�
                day = datetime.date(this_time.year, this_time.month, this_time.day)
                data_sample_ls.append(day.isoweekday())
                # ���㵱ǰʱ�κ�
                minute = this_time.hour * 60 + this_time.minute + this_time.second / 60
                if minute == 0:
                    data_sample_ls.append(1)
                else:
                    data_sample_ls.append(math.ceil(minute / 5))
                # add month
                data_sample_ls.append(this_time.month)
                # �����ӱ�ʱ������
                data_sample_ls.append(data[key])
                # ��������ÿһ������
                data_sample.append(data_sample_ls)
            save_name = './4.data_check/cangestion/flow_sample_data.csv'

        else:
            # �õ��żܳ��������Ķ�Ӧ�ֵ�
            data = dbf.get_disc_from_document(path, [2, 3, 0, 4, 5], encoding='utf-8', key_for_N=False, key_length=4,
                                              ifIndex=False, key_for_N_type='list', sign='_')
            # �������żܳ��ͣ�����ÿ����¼��ѵ������
            data_sample = []  # ���ڱ���ѵ������
            for key in data.keys():
                data_sample_ls = []  # ���ڱ��浥��һ��������
                key_list = key.split('_')
                if key_list[2] <= '2021-08-01 04:00:00':
                    continue
                data_sample_ls.extend(key_list)
                # ��ǰ4��Сʱ�������������ݻ�ȡ��
                this_time = datetime.datetime.strptime(key_list[2], '%Y-%m-%d %H:%M:%S')
                for i in range(48):
                    new_time = this_time + datetime.timedelta(minutes=(5 * (i + 1) * (-1)))
                    new_key = key_list[0] + '_' + key_list[1] + '_' + datetime.datetime.strftime(new_time,
                                                                                                 '%Y-%m-%d %H:%M:%S') + '_' + \
                              key_list[3]
                    try:
                        new_flow = data[new_key]
                        data_sample_ls.append(new_flow)
                    except:
                        data_sample_ls.append(0)
                # �жϵ�ǰ�����Ƿ�Ϊ�ڼ���,�Լ����������͵�ǰ�������ڽ׶�
                try:
                    holiday_list = holiday[key_list[2][:10].replace('-', '')]
                    data_sample_ls.append(1)
                    data_sample_ls.extend(holiday_list)
                except:
                    data_sample_ls.extend([0, 0, 0])
                # �ж��Ƿ��ǹ�����
                if is_workday(this_time):
                    data_sample_ls.append(1)
                else:
                    data_sample_ls.append(0)
                # �ж������ڼ�
                day = datetime.date(this_time.year, this_time.month, this_time.day)
                data_sample_ls.append(day.isoweekday())
                # ���㵱ǰʱ�κ�
                minute = this_time.hour * 60 + this_time.minute + this_time.second / 60
                if minute == 0:
                    data_sample_ls.append(1)
                else:
                    data_sample_ls.append(math.ceil(minute / 5))
                # add month
                data_sample_ls.append(this_time.month)
                # �����ӱ�ʱ������
                data_sample_ls.append(data[key])
                # ��������ÿһ������
                data_sample.append(data_sample_ls)
            if treat_type == 'enStation':
                save_name = './4.data_check/enStation_flow/flow_sample_data_enStation.csv'

            elif treat_type == 'exStation':
                save_name = './4.data_check/exStation_flow/flow_sample_data_exStation.csv'

    with open(save_name, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data_sample)


'''
    ����ʱ�䣺2022/9/14
    ���ʱ�䣺2022/9/14
    ���ܣ���������ģ�ͽ���һ��ʱ���żܻ����շ�վ������Ԥ��
    �ؼ��ʣ�����Ԥ�⡢����ģ�͡�һ��ʱ��
    �޸�ʱ�䣺
'''


def compute_future_flow_of_gantry_station(compute_no, key_name, this_time, basic_data=None, treat_type='gantry'):
    """
    ��������ģ�ͽ���һ��ʱ���żܻ����շ�վ������Ԥ��
    :param now_flow_dict:
    :param str this_time: ��ǰʱ��
    :param str key_name: �����ID
    :param str treat_type: ѡ��Ԥ�����"gantry"Ϊ����żܵģ�"station"Ϊ����շ�վ��"all"Ϊ�żܺ��շ�վ������
    :param int compute_no: ��ҪԤ���δ��ʱ�����
    :param list basic_data: ģ��Ԥ�����ʼ��������
    :return:
    """
    gantry_flow = {}  # �����ż�Ԥ����������
    station_flow = {}  # �����շ�վԤ����������
    vehicle_types = [['FLOW_1'], ['FLOW_2'], ['FLOW_3'], ['FLOW_4'], ['FLOW_11'], ['FLOW_12'], ['FLOW_13'], ['FLOW_14'],
                     ['FLOW_15'], ['FLOW_16']]

    # ��ȡ��һ���շ�վ����վԤ��ģ�Ͳ���
    if treat_type == 'gantry':
        basic_path = './Network_Parameter_old/gantry_model/' + key_name
    elif treat_type == 'station_in':
        basic_path = './Network_Parameter_old/station_model/in_model/' + key_name
    elif treat_type == 'station_out':
        basic_path = './Network_Parameter_old/station_model/out_model/' + key_name
    try:
        with open(basic_path + '/initial.pkl', 'rb') as file:
            data_initial = pickle.loads(file.read())
    except:
        print(key_name)
        return []
    # ��ȡ����ģ�Ͷ��󣬲����и���
    neural_network = neuralNetwork.neuralNetwork_2(data_initial['inputnodes'], data_initial['outputnodes'],
                                                   data_initial['hnodes_num'],
                                                   data_initial['hnodes_num_list'], data_initial['learningrate'],
                                                   data_initial['activation_type'])
    neural_network.load_paremeter(basic_path)

    for i in range(compute_no):
        # ��ȡʱ�����
        time_data = get_feature_of_flow_calculate(this_time, if_data=True)

        num = 0  #
        flow = []
        for k, vehicle_type in enumerate(vehicle_types):
            if treat_type == 'gantry':
                vehicle_type_list = [vehicle_type[0]]
                vehicle_type_list.extend(basic_data[key_name + '_' + vehicle_type[0]])
                vehicle_type_list.extend(time_data)
                if k == 0:
                    max_list = dbf.get_disc_from_document(
                        './Network_Parameter_old/gantry_model/' + key_name + '/max.csv',
                        [0, 1], encoding='utf-8', ifIndex=False, ifNoCol=True)
                    min_list = dbf.get_disc_from_document(
                        './Network_Parameter_old/gantry_model/' + key_name + '/min.csv',
                        [0, 1], encoding='utf-8', ifIndex=False, ifNoCol=True)
                for j, key in enumerate(max_list.keys()):
                    if min_list[key] == '0':
                        min_list[key] = 0
                    else:
                        min_list[key] = float(min_list[key])
                    vehicle_type_list[j] = (float(vehicle_type_list[j]) - float(min_list[key])) / (
                            float(max_list[key]) - float(min_list[key])) * 0.99 + 0.1
                flow = neural_network.query(vehicle_type_list)

            elif treat_type == 'station_in':
                vehicle_type_list = [vehicle_type[0]]
                vehicle_type_list.extend(basic_data[key_name + '_' + vehicle_type[0]])
                vehicle_type_list.extend(time_data)
                if k == 0:
                    max_list = dbf.get_disc_from_document(
                        './Network_Parameter_old/station_model/in_model/' + key_name + '/max.csv',
                        [0, 1], encoding='utf-8', ifIndex=False, ifNoCol=True)
                    min_list = dbf.get_disc_from_document(
                        './Network_Parameter_old/station_model/in_model/' + key_name + '/min.csv',
                        [0, 1], encoding='utf-8', ifIndex=False, ifNoCol=True)
                for j, key in enumerate(max_list.keys()):
                    if (float(max_list[key]) - float(min_list[key])) == 0:
                        vehicle_type_list[j] = (vehicle_type_list[j] - float(min_list[key])) * 0.99 + 0.1
                    else:
                        vehicle_type_list[j] = (vehicle_type_list[j] - float(min_list[key])) / (
                                float(max_list[key]) - float(min_list[key])) * 0.99 + 0.1

                # get
                flow = neural_network.query(vehicle_type_list)

            elif treat_type == 'station_out':
                vehicle_type_list = [vehicle_type[0]]
                vehicle_type_list.extend(basic_data[key_name + '_' + vehicle_type[0]])
                vehicle_type_list.extend(time_data)
                if k == 0:
                    max_list = dbf.get_disc_from_document(
                        './Network_Parameter_old/station_model/out_model/' + key_name + '/max.csv',
                        [0, 1], encoding='utf-8', ifIndex=False, ifNoCol=True)
                    min_list = dbf.get_disc_from_document(
                        './Network_Parameter_old/station_model/out_model/' + key_name + '/min.csv',
                        [0, 1], encoding='utf-8', ifIndex=False, ifNoCol=True)
                for j, key in enumerate(max_list.keys()):
                    if min_list[key] == '0':
                        min_list[key] = 0
                    else:
                        min_list[key] = float(min_list[key])
                    vehicle_type_list[j] = (vehicle_type_list[j] - float(min_list[key])) / (
                            float(max_list[key]) - float(min_list[key])) * 0.99 + 0.1
                flow = neural_network.query(vehicle_type_list)

            # ���õ���9λ��ת��Ϊ0��1��ֵ
            flow = [0 if i[0] < 0.5 else 1 for i in flow]
            # ��9λ0��1������תΪ�ַ���������ʮ����ת��
            pre_result_str = ''
            for j in flow:
                pre_result_str = pre_result_str + str(j)

            if treat_type == 'gantry':
                ls = basic_data[key_name + '_' + vehicle_type[0]]
                ls.pop(0)
                ls.append(int(pre_result_str, 2))
                basic_data[key_name + '_' + vehicle_type[0]] = ls
            # num += int(pre_result_str, 2)
            if treat_type == 'gantry':
                gantry_flow[this_time + '_' + str(vehicle_type[0])] = int(pre_result_str, 2)
            else:
                station_flow[this_time + '_' + str(vehicle_type[0])] = int(pre_result_str, 2)

        # ������������
        this_time = datetime.datetime.strftime(datetime.datetime.strptime(this_time, '%Y-%m-%d %H:%M:%S') +
                                               datetime.timedelta(minutes=5), '%Y-%m-%d %H:%M:%S')

    if treat_type == 'gantry':
        return gantry_flow
    elif treat_type == 'station_in' or treat_type == 'station_out':
        return station_flow
    else:
        return gantry_flow, station_flow


'''
    ����ʱ�䣺2022/9/14
    ���ʱ�䣺2022/9/14
    ���ܣ��ж�·��ӵ�µĵȼ�(���ݳ�����ӵ���ж��㷨�������)
    �ؼ��ʣ�ӵ�µȼ���ӵ���ж�
    �޸�ʱ�䣺No.1 2022-10-11,
'''


def charge_congestion_level_by_result(congestion_charge_speed, congestion_charge_flow, congestion_charge_have,
                                      gantry_have_data, this_time):
    """
    �ж�·��ӵ�µĵȼ�(���ݳ�����ӵ���ж��㷨�������)
    :param this_time:
    :param gantry_have_data:
    :param congestion_charge_speed:
    :param congestion_charge_flow: ���ݵ�·�����������ƥ��õ���ӵ���ж����
    :param congestion_charge_have: ���ݵ�·�������õ���ӵ���ж����
    :return:
    """

    vehicle_types = [['1'], ['2'], ['3'], ['4'], ['11'], ['12'], ['13'], ['14'], ['15'], ['16']]
    level_data = []  # �����·��ӵ�������Ϣ
    for key in congestion_charge_speed.keys():
        gantry_have_data_new = {}
        # ����ٶ��ж�Ϊӵ�����
        if congestion_charge_speed[key] == 1:
            for i, vtype in enumerate(vehicle_types):
                gantry_have_data_new[vtype[0]] = gantry_have_data[key + '_' + this_time][i]  # changed at 2023/01/29
            # print('speed ' + key)
            # print(gantry_have_data_new)
            length = mcm.monte_carlo_main(gantry_have_data_new, [1, 0])
            try:
                # ����������ж�����Ӧ������������Ӧӵ�µȼ���ֵ
                charge_have = congestion_charge_have[key]
                if charge_have == 2:
                    level_data.append([key, 5, 2, length])
                elif charge_have == 1:
                    level_data.append([key, 4, 2, length])
                elif length > 600:  # bug
                    level_data.append([key, 3, 2, length])
                else:
                    level_data.append([key, 0, 0, length])

            except:
                level_data.append([key, 4, 2, length])
        # ����ٶ��ж�Ϊ��ӵ�����
        else:
            try:
                # �����������ж�����Ӧ���
                charge_flow = congestion_charge_flow[key]
                if charge_flow == 1:
                    for vtype in vehicle_types:
                        try:
                            gantry_have_data_new[vtype[0]] = gantry_have_data[key + '_' + vtype[0]]
                        except:
                            gantry_have_data_new[vtype[0]] = 0
                    # print('flow ' + key)
                    # print(gantry_have_data_new)
                    length = mcm.monte_carlo_main(gantry_have_data_new, [1, 0])
                    try:
                        charge_have = congestion_charge_have[key]
                        if charge_have == 2:
                            level_data.append([key, 5, 2, length])
                        elif charge_have == 1:
                            level_data.append([key, 4, 2, length])
                        elif length > 600:  # bug
                            level_data.append([key, 3, 2, length])
                        else:
                            level_data.append([key, 0, 0, length])
                    except:
                        level_data.append([key, 4, 2, length])
                else:
                    try:
                        charge_have = congestion_charge_have[key]
                        if charge_have == 2:
                            level_data.append([key, 3, 1, 0])
                        elif charge_have == 1:
                            level_data.append([key, 2, 1, 0])
                        else:
                            level_data.append([key, 0, 0, 0])
                    except:
                        level_data.append([key, 4, 2, 0])
            except:
                # �����������ж�û����Ӧ�����Ĭ�ϸ�ֵΪ��ӵ��
                level_data.append([key, 0, 0, 0])

    return level_data


'''
    ����ʱ�䣺2022/10/08
    ���ʱ�䣺2022/10/08
    ���ܣ������е��ٶ����ݣ�����ʱ��ͳ��ͽ���ƽ������
    �ؼ��ʣ��ٶȣ�ƽ��
    �޸�ʱ�䣺
'''


def compute_avg_data_of_speed():
    """
    �����е��ٶ����ݣ�����ʱ��ͳ��ͽ���ƽ������
    :return:
    """
    # �õ��ٶȵ��ֵ�
    gantry_service_list = dbf.get_disc_from_document('./4.statistic_data/basic_data/speed_origin_data_time.csv',
                                                     [0, 2, 1, 3], encoding='gbk', key_for_N=True, key_length=3,
                                                     ifIndex=False, key_for_N_type='list', sign='_')
    # gantry_service_list = dbf.get_disc_from_document(data,
    #                                                  [0, 2, 1, 3], key_for_N=True, key_length=3,
    #                                                  ifIndex=False, key_for_N_type='list', sign='_', input_type='list')

    # ����ƽ������
    data_result = dbf.compute_dict_by_group(gantry_service_list, [], 'avg', '')
    dbf.basic_save_dict_data(data_result, './4.statistic_data/basic_data/speed_origin_data_avg.csv', True, True)


"""
ls
"""


def get_last_time_inout_data(path, time_no, this_time):
    data_dict = {}
    with open(path) as f:
        for i, row in enumerate(f):
            row = row.split(',')
            row[-1] = row[-1][:-1]
            if i > 0:
                try:
                    data_dict[row[1] + '_in'].append([row[0] + '_in', float(row[3])])
                except:
                    data_dict[row[1] + '_in'] = [[row[0] + '_in', float(row[3])]]
                try:
                    data_dict[row[1] + '_out'].append([row[0] + '_out', float(row[4])])
                except:
                    data_dict[row[1] + '_out'] = [[row[0] + '_out', float(row[4])]]
    data_time_dict = {}
    for i in range(time_no):
        t = datetime.datetime.strftime(datetime.datetime.strptime(this_time, '%Y-%m-%d %H:%M:%S') +
                                       datetime.timedelta(minutes=5 * (time_no - i - 1) * (-1)),
                                       '%Y-%m-%d %H:%M:%S')
        for j in range(len(data_dict[t + '_in'])):
            try:
                data_time_dict[data_dict[t + '_in'][j][0]].append(data_dict[t + '_in'][j][1])
            except:
                data_time_dict[data_dict[t + '_in'][j][0]] = [data_dict[t + '_in'][j][1]]
        for j in range(len(data_dict[t + '_out'])):
            try:
                data_time_dict[data_dict[t + '_out'][j][0]].append(data_dict[t + '_out'][j][1])
            except:
                data_time_dict[data_dict[t + '_out'][j][0]] = [data_dict[t + '_out'][j][1]]
    return data_time_dict


"""
ls
"""


def get_last_flow_data(data, this_time):
    data_dict = {}
    for i in range(len(data)):
        data_dict[data[i][0]] = data[i][2:51]
    return data_dict


if __name__ == '__main__':
    # quick()

    # statistic the time of two gantry
    # path_list = []
    # # paths = dop.path_of_holder_document('./2.Middle_Data/202207', True)
    # paths = dop.path_of_holder_document('./2.Middle_Data/202207', True)
    # paths.extend(dop.path_of_holder_document('./2.Middle_Data/202206', True))
    # paths.extend(dop.path_of_holder_document('./2.Middle_Data/202205', True))
    # paths.extend(dop.path_of_holder_document('./2.Middle_Data/202204', True))
    # paths.extend(dop.path_of_holder_document('./2.Middle_Data/202203', True))
    # paths.extend(dop.path_of_holder_document('./2.Middle_Data/202202', True))
    # paths.extend(dop.path_of_holder_document('./2.Middle_Data/202201', True))
    # paths.extend(dop.path_of_holder_document('./2.Middle_Data/202112', True))
    # paths.extend(dop.path_of_holder_document('./2.Middle_Data/202111', True))
    # paths.extend(dop.path_of_holder_document('./2.Middle_Data/202110', True))
    # data = []
    # for path in paths:
    #     if '20211008' < path[-12:-4] < '20220730':
    #         path_list.append(path)
    # # dbf.get_result_from_data_with_colValue(path_list, ['�շѵ�Ԫ·��'], )
    # result = get_list_of_gantry_path(paths, '�ż�·��', '�ż�ʱ�䴮', '��ڳ���', '���ڳ���', 'dict')
    #
    # statistic_time_between_gantrys(result)
    #
    # gantrys = ["G003061003000120", "G003061003000210", "G003061003000120", "G003061003000310", "G003061003000220",
    #            "G003061003000410", "G003061003000320", "G003061003000510", "G003061003000420", "G003061003000520",
    #            "G003061003000710", "G003061003000720", "G003061003000810", "G003061003000820", "G003061003001310",
    #            "G003061003001320", "G003061003001410", "G003061003001420"]
    #
    # get_data_of_two_gantry_have(path_list, )
    #
    # statistic_the_two_gantry_data()

    # get_real_flow_with_time_revise(path_list)

    # chect_the_flow_data()
    # gantrys = ["G003061003000120", "G003061003000210", "G003061003000110", "G003061003000310", "G003061003000220",
    #            "G003061003000410", "G003061003000320", "G003061003000510", "G003061003000420", "G003061003000520",
    #            "G003061003000710", "G003061003000720", "G003061003000810", "G003061003000820", "G003061003001310",
    #            "G003061003000610", "G003061003000620", "G003061003000910", "G003061003000920", "G003061003001010",
    #            "G003061003001020", "G003061003001120", "G003061003001110", "G003061003001220", "G003061003001210",
    #            "G003061003001320", "G003061003001410", "G003061003001420", "G003061003001510", "G003061003001520",
    #            "G003061003001610", "G003061003001620", "G003061003001710", "G003061003001720"]
    paths = dop.path_of_holder_document('./1.Gantry_Data/202205/gantry_path/', True)
    # paths.extend(dop.path_of_holder_document('./1.Gantry_Data/202109/gantry_path/', True))
    # paths.extend(dop.path_of_holder_document('./1.Gantry_Data/202110/gantry_path/', True))
    # paths.extend(dop.path_of_holder_document('./1.Gantry_Data/202111/gantry_path/', True))
    # paths.extend(dop.path_of_holder_document('./1.Gantry_Data/202112/gantry_path/', True))
    # paths.extend(dop.path_of_holder_document('./1.Gantry_Data/202201/gantry_path/', True))
    # paths.extend(dop.path_of_holder_document('./1.Gantry_Data/202202/gantry_path/', True))
    # paths.extend(dop.path_of_holder_document('./1.Gantry_Data/202203/gantry_path/', True))
    # paths.extend(dop.path_of_holder_document('./1.Gantry_Data/202204/gantry_path/', True))
    # paths.extend(dop.path_of_holder_document('./1.Gantry_Data/202205/gantry_path/', True))
    # paths.extend(dop.path_of_holder_document('./1.Gantry_Data/202206/gantry_path/', True))
    # paths.extend(dop.path_of_holder_document('./1.Gantry_Data/202207/gantry_path/', True))
    # paths.extend(dop.path_of_holder_document('./1.Gantry_Data/202208/gantry_path/', True))

    # get_real_flow_with_time_revise(paths, filter_id=gantrys)
    get_real_flow_with_time_revise(paths)

    # check_gantry_flow_data_after_revise()

    # get_feature_of_flow_calculate('./4.data_check/enStation_flow/enStation_go_type_flow.csv', 'enStation')

    # train_neuralNetwork_model('./4.data_check/cangestion/flow_sample_data.csv')

    # train_neuralNetwork_model('./4.data_check/cangestion/flow_sample_data.csv')
    # train_neuralNetwork_model('./4.data_check/enStation_flow/flow_sample_data_enStation.csv', 'station_in')
    # train_neuralNetwork_model('./4.data_check/exStation_flow/flow_sample_data_exStation.csv', 'station_out')

    # compute_history_num_of_gantry('2022-05-01', '2022-08-18')

    # compute_avg_data_of_speed()

    # compute_DTW_of_gantrys('2022-08-05', '2022-08-08')

    # get_DTW_threshold_of_gantrys(0.008)

    # get_have_threshold_of_gantrys()
