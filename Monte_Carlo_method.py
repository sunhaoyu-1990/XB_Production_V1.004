# coding=gbk
"""
���ؿ����㷨�ĵײ�ʵ�ֺ���
�ĵ�����ʱ�䣺2022/9/23
�ĵ��޸�ʱ�䣺
"""

# �����㷨��
import csv
import datetime
import Data_Basic_Function as dbf
import Keyword_and_Parameter as kp


'''
    ����ʱ��: 2022/9/23
    ���ʱ��: 2022/9/26
    ����: �״����������⺯��
    �ؼ��ʣ��״Ρ�������
    �޸�ʱ�䣺
'''


def create_sample_first(key_word, sample_num, sample_features, sample_rate=None):
    """
    �״����������⺯��
    :param list key_word: ������ɵĸ����������Ĺؼ�����
    :param list sample_num: �趨���ؼ���������Ҫ���ɵĸ���
    :param list sample_features: ���������Ĳ���ֵֵ��
    :param list sample_rate: ѡ��ֵ���ڸ������ĸ��ʣ���Ϊ��ʱ����ʾ����Ϊƽ����
    :return:
    """
    # ѭ���������е�����
    sample_data = []  # ���ڱ�����������
    for k, key in enumerate(key_word):
        if sample_num[k] != 0:
            for i in range(int(sample_num[k])):
                # �ж��Ƿ��������ѡ�и���Ϊͬ����
                if len(sample_rate) == 0:
                    rate = [1.0 / len(sample_features) for j in range(len(sample_features))]
                    sample_data.append([key, dbf.get_feature_random(sample_features, rate)])
                else:
                    # ������ʲ���ͬ
                    sample_data.append([key, dbf.get_feature_random(sample_features, sample_rate)])
        else:
            pass
    return sample_data


'''
    ����ʱ��: 2022/9/26
    ���ʱ��: 2022/9/26
    ����: ��������������������������
    �ؼ��ʣ������⣬������
    �޸�ʱ�䣺No.1 2022/9/27,���������������Ϊ���ֵĿ�����
'''


def create_sample_feature(sample_data, sample_features, sample_rate=None, random_type='', feature_type='str'):
    """
    ��������������������������
    :param str random_type: �������������
    :param str feature_type: �ж�����Ľ�������ͣ�strΪ�ַ�����intΪ���ͣ�floatΪ������
    :param list sample_data: �Ѿ����ɵ�����������
    :param list sample_features: ���������Ĳ���ֵֵ�򣬵�feature_type����strʱ��Ϊ�ֵ����ͣ���Ÿ��ؼ������Ͷ�Ӧ���������
    :param list sample_rate: ѡ��ֵ���ڸ������ĸ��ʣ���Ϊ��ʱ����ʾ����Ϊƽ����
    :return:
    """
    # �������е������������ݣ���������µ�����ֵ
    for i in range(len(sample_data)):
        if feature_type == 'str':
            if len(sample_rate) == 0:
                rate = [1.0 / len(sample_features) for i in range(len(sample_features))]
                sample_data[i].append(dbf.get_feature_random(sample_features, rate, random_type, feature_type))
            else:
                # ������ʲ���ͬ
                sample_data[i].append(dbf.get_feature_random(sample_features, sample_rate, random_type, feature_type))
        else:
            # ���ݸ�����¼�¹ؼ������ݣ������Ӧ���������
            sample_data[i].append(
                dbf.get_feature_random(sample_features[int(sample_data[i][0])], sample_rate, random_type, feature_type))
    return sample_data


'''
    ����ʱ��: 2022/9/26
    ���ʱ��: 2022/9/27
    ����: ��ģ�����ɵ������������м��㣬���Ŀ��ֵ
    �ؼ��ʣ��������㣬Ŀ��ֵ
    �޸�ʱ�䣺
'''


def compute_sample_feature(sample_data, index, filter_num, treat_type='add'):
    """
    ��ģ�����ɵ������������м��㣬���Ŀ��ֵ
    :param str treat_type: �Ը������ļ��㷽ʽ
    :param list sample_data: ��������
    :param int index: ���ݹ��˵��ֶ�
    :param filter_num: ���ݹ����ж���ֵ
    :return:
    """
    # �������е�����
    sum_num = 0  # ���ڱ������ݵ��ܺ�
    count_num = 0  # ���ڱ������ݵ��ܸ���
    for i in range(len(sample_data)):
        # �������ݹ���,���������Ž��м���
        if sample_data[i][index] == filter_num:
            if treat_type == 'add':
                for j in range(2, len(sample_data[i])):
                    sum_num += sample_data[i][j]

            elif treat_type == 'avg':
                for j in range(2, len(sample_data[i])):
                    sum_num += sample_data[i][j]
                    count_num += 1
    # ������
    if treat_type == 'add':
        return sum_num
    elif treat_type == 'avg':
        return sum_num / count_num


'''
    ����ʱ��: 2022/9/26
    ���ʱ��: 2022/9/27
    ����: ���ؿ����㷨single����
    �ؼ��ʣ�single����
    �޸�ʱ�䣺
'''


def monte_carlo_single(flow_data, cut_rate):
    """
    ��ģ�����ɵ������������м��㣬���Ŀ��ֵ
    :param dict flow_data: �����Ͷ�Ӧ����������
    :param list cut_rate: �����¹ʵ�λ�ü�������ĳ����ֲ�����
    :return:
    """
    # �����Ͷ�Ӧ�ĳ�����Χ
    vehicle_length = kp.get_parameter_with_keyword('vehicle_length')
    # �����Ͷ�Ӧ�ĳ�����Χ
    gap_length = kp.get_parameter_with_keyword('gap_length')

    # ������������ֵ
    vehicle_type = ['1', '2', '3', '4', '11', '12', '13', '14', '15', '16']

    # ���ɸ����Ͷ�Ӧ����������
    vehicle_num = [0 if flow_data[v_type] < 0 else flow_data[v_type] for v_type in vehicle_type]

    # ������������
    sample_data = create_sample_first(vehicle_type, vehicle_num, [1, 0], cut_rate)

    # ���������ĳ���������
    sample_data = create_sample_feature(sample_data, vehicle_length, random_type='normality', feature_type='float')

    # ���������ĳ���೤������
    sample_data = create_sample_feature(sample_data, gap_length, random_type='normality', feature_type='float')

    # �������������ļ���
    total_length = compute_sample_feature(sample_data, 1, 1, 'add')

    # ���������������д�����������ս��
    return round(total_length / 4, 1)


'''
    ����ʱ��: 2022/10/12
    ���ʱ��: 2022/10/12
    ����: ���ؿ����㷨�ܺ���
    �ؼ��ʣ��ܺ���
    �޸�ʱ�䣺
'''


def monte_carlo_main(flow_data, cut_rate, simple_num=500):
    """
    ���ؿ����㷨�ܺ���
    :param dict flow_data: �����Ͷ�Ӧ����������
    :param list cut_rate: �����¹ʵ�λ�ü�������ĳ����ֲ�����
    :param int simple_num:
    :return:
    """
    simple_data = []
    for i in range(simple_num):
        simple_data.append(monte_carlo_single(flow_data, cut_rate))
    # return round(sum(simple_data) / simple_num, 1)
    return max(simple_data)


if __name__ == '__main__':
    vehicle_type = ['1', '2', '3', '4', '11', '12', '13', '14', '15', '16']
    have_data = dbf.get_disc_from_document('./4.statistic_data/basic_data/820_have_type.csv',
                                                      [0, 1, 2, 5], encoding='utf-8', ifIndex=False, key_length=3, key_for_N=False)
    data_result = []
    start_time = '2022-08-07 00:00:00'
    end_time = '2022-08-07 23:55:00'
    now_time = '2022-08-07 00:00:00'
    for i in range(288):
        if now_time > end_time:
            break
        print(now_time)
        type_dict = {}
        for vt in vehicle_type:
            try:
                type_dict[vt] = float(have_data['G003061003000820-' + now_time + '-' + vt])
            except:
                type_dict[vt] = 0

        # vehicle_flow = {'1': 2120, '2': 5, '3': 40, '4': 20, '11': 25, '12': 20, '13': 41, '14': 5, '15': -3, '16': 0, '21': 0, '22': 0, '23': 0,
        #                 '24': 0, '25': 0, '26': 0}
        data_result.append(['G003061003000820', now_time, monte_carlo_main(type_dict, [1, 0])])
        now_time = datetime.datetime.strftime(datetime.datetime.strptime(now_time, '%Y-%m-%d %H:%M:%S') +
                                              datetime.timedelta(minutes=5), '%Y-%m-%d %H:%M:%S')

    with open('./4.statistic_data/basic_data/820_length_ls.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data_result)
