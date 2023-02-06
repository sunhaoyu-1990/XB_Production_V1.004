# coding=gbk
import datetime
import time

import pandas as pd

import GetData as gd
import Document_process as dop
import Keyword_and_Parameter as kp
import Data_Basic_Function as dbf

'''
    �ĵ�����ʱ�䣺2022/05/08
    1.�ռ����յ���ͨ���շ����ݣ����б��ر��棬�����ַͨ��Keyword_and_Parameter�ؼ��ֹ�������
    2.ʵ�ָ�������ݵ����ݿ��ϴ�
'''

'''
    ����ʱ�䣺2022/05/08
    ���ʱ�䣺2022/05/08
    ���ܣ��������յ���ͨ���շ����ݣ����б��ر���
    �޸�ʱ�䣺
'''


def get_yesterday_data():
    # ��ȡ�����ĺ��շ��������ݿ����
    LT_department_parameter = kp.get_parameter_with_keyword('LT_department_mysql')
    vehicle_charge_parameter = kp.get_parameter_with_keyword('vehicle_charge_mysql')
    # �����������������ݿ��mysql����
    db_LT = gd.OperationMysql(LT_department_parameter['host'], LT_department_parameter['port'],
                              LT_department_parameter['user'], LT_department_parameter['passwd'],
                              LT_department_parameter['db'])
    # ���������շ��������ݿ��mysql����
    db_charge = gd.OperationMysql(vehicle_charge_parameter['host'], vehicle_charge_parameter['port'],
                                  vehicle_charge_parameter['user'], vehicle_charge_parameter['passwd'],
                                  vehicle_charge_parameter['db'])
    # ��ȡ�������ݵ���ֹʱ��
    time_now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    yesterday_time = datetime.datetime.strptime(time_now, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(days=-1)
    start_time = yesterday_time.strftime("%Y-%m-%d %H:%M:%S")[:-8] + '00:00:00'
    end_time = yesterday_time.strftime("%Y-%m-%d %H:%M:%S")[:-8] + '23:59:59'

    # ��ȡ��ͨ���շ����ݵĴ洢λ����Ϣ
    LT_path = kp.get_parameter_with_keyword('LT_oneDay_save_path')
    gantry_path = kp.get_parameter_with_keyword('gantry_oneDay_save_path')
    gantry_path_pro = kp.get_parameter_with_keyword('gantry_oneDay_save_path_pro')
    entry_path = kp.get_parameter_with_keyword('in_oneDay_save_path')
    exit_path = kp.get_parameter_with_keyword('out_oneDay_save_path')
    iden_path = kp.get_parameter_with_keyword('iden_oneDay_save_path')

    # ��ȡ��ͨ���շ����ݵı�ע�洢λ����Ϣ
    LT_back_path = kp.get_parameter_with_keyword('LT_oneDay_back_path')
    gantry_back_path = kp.get_parameter_with_keyword('gantry_oneDay_back_path')
    gantry_back_path_pro = kp.get_parameter_with_keyword('gantry_oneDay_back_path_pro')
    entry_back_path = kp.get_parameter_with_keyword('in_oneDay_back_path')
    exit_back_path = kp.get_parameter_with_keyword('out_oneDay_back_path')
    iden_back_path = kp.get_parameter_with_keyword('iden_oneDay_back_path')

    # ��ȡ��ͨ���շ����ݵ�sql���
    LT_sql = kp.get_parameter_with_keyword('LT_features')
    gantry_sql = kp.get_parameter_with_keyword('charge_gantry_features')
    gantry_sql_pro = kp.get_parameter_with_keyword('charge_gantry_features_pro')
    entry_sql = kp.get_parameter_with_keyword('charge_in_features')
    exit_sql = kp.get_parameter_with_keyword('charge_out_features')
    iden_sql = kp.get_parameter_with_keyword('charge_iden_features')

    # �ֱ��ȡ��ֹʱ��ĸ������ݿ����ݲ����б���
    db_LT.get_data_in_for(start_time, end_time, LT_path, save_type='oneData', get_type='LT', sql_str=LT_sql,
                          back_path=LT_back_path)
    # �ż����ݻ�ȡ������
    db_charge.get_data_in_for(start_time, end_time, gantry_path, save_type='oneData', get_type='gantry',
                              sql_str=gantry_sql, back_path=gantry_back_path)
    # ʡ���ż����ݻ�ȡ������
    db_charge.get_data_in_for(start_time, end_time, gantry_path_pro, save_type='oneData', get_type='gantry',
                              sql_str=gantry_sql_pro, back_path=gantry_back_path_pro)
    # ������ݻ�ȡ������
    db_charge.get_data_in_for(start_time, end_time, exit_path, save_type='oneData', get_type='entry',
                              sql_str=exit_sql, back_path=exit_back_path)
    # �������ݻ�ȡ������
    db_charge.get_data_in_for(start_time, end_time, entry_path, save_type='oneData', get_type='exit',
                              sql_str=entry_sql, back_path=entry_back_path)


'''
    ����ʱ�䣺2022/2/21
    ���ʱ�䣺2022/2/21
    ���ܣ��������㷨���׶ε�����д�����ݿ�
    �޸�ʱ�䣺No.1
'''


def save_data_to_Mysql(write_type, upload_data):
    """
    �������㷨���׶ε�����д�����ݿ�
    :param write_type:д�����ݵ����ͣ�middle����Ϊ�м�����д�룬timePart����һ��ʱ��ϲ����������д�룬checkFeature����������������д�룬checkResult���������д��
    :param upload_data:��Ҫд�������
    :return:
    """
    # ��������������mysql���ݿ����ӵĶ���
    db = gd.OperationMysql('192.168.0.182', 3306, 'root', '123456', 'vehicle_check')
    # ���write_typeΪmiddle, ������м����ݽ��б���
    if write_type == 'middle':
        db.write_one('middle_data',
                     ["TID", "PASSID", "enVehiclePlateTotal", "enIdenVehiclePlate", "enVehiclePlate", "enPlateColor",
                      "entryStationID", "entryStationHEX", "entryTime", "enMediaType", "enVehicleType",
                      "enVehicleClass", "entryWeight", "enAxleCount", "exVehiclePlateTotal", "exIdenVehiclePlate",
                      "exVehiclePlate", "exPlateColor", "exitStationID", "exitTime", "exMediaType", "exVehicleType",
                      "exVehicleClass", "exitWeight", "exAxleCount", "obuVehicleType", "obuSn", "etcCardId",
                      "ExitFeeType", "exOBUVehiclePlate", "exCPUVehiclePlate", "payFee", "middle_type", "gantryPath",
                      "intervalPath", "gantryNum", "gantryTimePath", "gantryTypePath", "gantryTotalFee",
                      "gantryTotalLength", "firstGantryTime", "endGantryTime"],
                     upload_data,
                     ['varchar', 'varchar', 'varchar', 'varchar', 'tinyint', 'varchar', 'datetime', 'datetime', 'float',
                      'float', 'float', 'float', 'int', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar',
                      'datetime', 'float', 'float', 'float', 'float', 'int', 'int', 'varchar', 'varchar', 'int',
                      'varchar', 'varchar', 'float', 'varchar', 'varchar', 'varchar', 'int', 'varchar', 'varchar',
                      'float', 'int', 'datetime', 'datetime'])
    # ���write_typeΪtimePart, �����һ��ʱ��ĺϲ����ݽ��б���
    elif write_type == 'timePart':
        db.write_one('partTimeConcat_data_20220511',
                     ["PASSID", "enVehiclePlateTotal", "enIdenVehiclePlate", "enVehiclePlate", "enPlateColor",
                      "entryStationID", "entryStationHEX", "entryTime", "enMediaType", "enVehicleType",
                      "enVehicleClass", "entryWeight", "enAxleCount", "exVehiclePlateTotal", "exIdenVehiclePlate",
                      "exVehiclePlate", "exPlateColor", "exitStationID", "exitTime", "exMediaType", "exVehicleType",
                      "exVehicleClass", "exitWeight", "exAxleCount", "obuVehicleType", "obuSn", "etcCardId",
                      "ExitFeeType", "exOBUVehiclePlate", "exCPUVehiclePlate", "payFee", "middle_type", "gantryPath",
                      "intervalPath", "gantryNum", "gantryTimePath", "gantryTypePath", "gantryTotalFee",
                      "gantryFeePath",
                      "gantryTotalLength", "firstGantryTime", "endGantryTime", "ifProvince", "endsGantryType",
                      "ifHaveCard",
                      "vehiclePlate", "vehiclePlateTotal", "endTime", "vehicleType", "dataType", "proInGantryId",
                      "proInStationId",
                      "proInStationName", "inProTime", "proOutGantryId", "proOutStationId", "proOutStationName",
                      "outProTime"],
                     upload_data,
                     ['varchar', 'varchar', 'varchar', 'varchar', 'tinyint', 'varchar', 'varchar', 'datetime', 'int',
                      'int', 'int', 'int', 'int', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar',
                      'datetime', 'int', 'int', 'int', 'int', 'int', 'int', 'varchar', 'varchar', 'int',
                      'varchar', 'varchar', 'int', 'varchar', 'varchar', 'varchar', 'int', 'varchar', 'varchar',
                      'int', 'varchar', 'int', 'datetime', 'datetime', 'tinyint', 'varchar', 'tinyint', 'varchar',
                      'varchar', 'datetime', 'float', 'varchar', 'varchar', 'varchar', 'varchar', 'datetime', 'varchar',
                      'varchar', 'varchar', 'datetime'])
    # ���write_typeΪcheckFeature, ����Ի����������������ݽ��б���
    elif write_type == 'checkFeature':
        db.write_one("checkFeature_data_20220511",
                     ["PASSID", "shortPathFee", "shortPathLength", "shortPath", "shortPathNum", "ifVehiclePlateMatch",
                      "ifVehiclePlateSame", "ifPlateColorSame", "ifOBUPlateSame", "ifETCPlateSame", "ifVehicleTypeSame",
                      "ifOBUTypeLarger", "ifUseShortFee", "ifVeTypeLargerAxle", "gantryNumRate", "ifGantryPathWhole",
                      "gantryPathIntegrity", "gantryPathMatch", "ifGrantryFeeSame", "ifShortFeeSame", "maxOutRangeTime",
                      "ifTimeOutRange", "maxOutRangeGantry", "outRangeTimePath", "outRangeGantryPath",
                      "outRangeSpeedPath", "totalTime", "ifShortTimeAbnormal", "shortOutRangeTime", "pathType"],
                     upload_data,
                     ['varchar', 'int', 'int', 'varchar', 'int', 'tinyint', 'tinyint', 'tinyint', 'tinyint',
                      'tinyint', 'tinyint', 'tinyint', 'tinyint', 'tinyint', 'float', 'tinyint', 'int', 'int',
                      'tinyint', 'tinyint', 'int', 'tinyint', 'varchar', 'varchar', 'varchar', 'varchar', 'int',
                      'tinyint', 'int', 'varchar'])
    # ���write_typeΪcheckResult, ����Ի��������ݽ��б���
    elif write_type == 'checkResult':
        db.write_one('checkResult_data_20220511',
                     ["PASSID", "GantryPathAbnormal", "ifVehiclePlateMatchCode", "ifVehiclePlateSameCode",
                      "ifPlateColorSameCode",
                      "ifOBUPlateSameCode", "ifETCPlateSameCode", "ifVehicleTypeSameCode", "ifExAxleLargerCode",
                      "ifOBUTypeLargerCode", "ifPassTimeLarger3DaysCode", "ifVehicleTypeSameCode_filter",
                      "ifPassTimeAbnormalCode", "ifFeeMatchCode", "ifPathTypeAbnormalCode", "combineCode",
                      "abnormalType", "abnormalCore"],
                     upload_data,
                     ['varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar',
                      'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar',
                      'varchar', 'float'])
    # ���write_typeΪcheckTotal, ����Ի��������ݽ��б���
    elif write_type == 'checkTotal':
        db.write_one('vehicleCheckTotal_data_20220320',
                     ["PASSID", "enVehiclePlateTotal", "enIdenVehiclePlate", "enVehiclePlate", "enPlateColor",
                      "entryStationID", "entryStationHEX", "entryTime", "enMediaType", "enVehicleType",
                      "enVehicleClass", "entryWeight", "enAxleCount", "exVehiclePlateTotal", "exIdenVehiclePlate",
                      "exVehiclePlate", "exPlateColor", "exitStationID", "exitTime", "exMediaType", "exVehicleType",
                      "exVehicleClass", "exitWeight", "exAxleCount", "obuVehicleType", "obuSn", "etcCardId",
                      "ExitFeeType", "exOBUVehiclePlate", "exCPUVehiclePlate", "payFee", "middle_type", "gantryPath",
                      "intervalPath", "gantryNum", "gantryTimePath", "gantryTypePath", "gantryTotalFee",
                      "gantryTotalLength", "firstGantryTime", "endGantryTime", "ifProvince", "endsGantryType",
                      "vehiclePlate", "vehiclePlateTotal", "endTime", "dataType",

                      "shortPathFee", "shortPathLength", "shortPath", "shortPathNum", "ifVehiclePlateMatch",
                      "ifVehiclePlateSame", "ifPlateColorSame", "ifOBUPlateSame", "ifETCPlateSame", "ifVehicleTypeSame",
                      "ifOBUTypeLarger", "ifUseShortFee", "ifVeTypeLargerAxle", "gantryNumRate", "ifGantryPathWhole",
                      "gantryPathIntegrity", "gantryPathMatch", "ifGrantryFeeSame", "ifShortFeeSame", "maxOutRangeTime",
                      "ifTimeOutRange", "maxOutRangeGantry", "totalTime", "ifShortTimeAbnormal", "shortOutRangeTime",
                      "pathType",

                      "GantryPathAbnormal", "ifVehiclePlateMatchCode", "ifVehiclePlateSameCode", "ifPlateColorSameCode",
                      "ifOBUPlateSameCode", "ifETCPlateSameCode", "ifVehicleTypeSameCode", "ifExAxleLargerCode",
                      "ifOBUTypeLargerCode", "ifPassTimeLarger3DaysCode", "ifVehicleTypeSameCode_filter",
                      "ifPassTimeAbnormalCode", "ifFeeMatchCode", "ifPathTypeAbnormalCode", "combineCode",
                      "abnormalType", "abnormalCore"],
                     upload_data,
                     ['varchar', 'varchar', 'varchar', 'varchar', 'tinyint', 'varchar', 'datetime', 'datetime', 'float',
                      'float', 'float', 'float', 'int', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar',
                      'datetime', 'float', 'float', 'float', 'float', 'int', 'int', 'varchar', 'varchar', 'int',
                      'varchar', 'varchar', 'float', 'varchar', 'varchar', 'varchar', 'int', 'varchar', 'varchar',
                      'float', 'int', 'datetime', 'datetime', 'tinyint', 'varchar', 'varchar', 'varchar', 'datetime',
                      'varchar',

                      'float', 'float', 'varchar', 'float', 'tinyint', 'tinyint', 'tinyint', 'tinyint',
                      'tinyint', 'tinyint', 'tinyint', 'tinyint', 'tinyint', 'float', 'tinyint', 'float', 'float',
                      'tinyint', 'tinyint', 'float', 'tinyint', 'varchar', 'float', 'tinyint', 'float', 'varchar',

                      'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar',
                      'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar',
                      'varchar', 'float'])
    # ���write_typeΪcheckResult, ����Ի��������ݽ��б���
    elif write_type == 'tollinterval':
        db.write_one('tollinterval',
                     ["id", "intervalID", "name", "type", "length", "startLat",
                      "startLng", "startStakeNum", "endStakeNum", "endLat", "endLng", "tollRoads", "endTime",
                      "provinceType", "operation", "isLoopCity", "enTollStation", "exTollStation",
                      "entrystation", "exitstation", "tollGrantry", "ownerid", "roadid", "roadidname", "roadtype",
                      "feeKtype", "feeHtype", "status", "Gantrys", "inoutprovince", "HEX", "NOTE", "SORT", "DIRECTION",
                      "BEGINTIME", "VERTICALSECTIONTYPE", "tollstaion"],
                     upload_data,
                     ['varchar', 'varchar', 'varchar', 'int', 'int', 'float', 'float', 'float', 'float', 'float',
                      'float', 'varchar',
                      'datetime', 'int', 'int', 'int', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'int',
                      'int',
                      'varchar', 'int', 'int', 'int', 'int', 'varchar', 'int', 'varchar', 'varchar', 'varchar', 'int',
                      'datetime', 'int', 'varchar'])

    # ���write_typeΪcheckTotal, ����Ի��������ݽ��б���
    elif write_type == 'totaldata':
        totaldata_LT_table_name = kp.get_parameter_with_keyword('totaldata_LT_table_name')
        totaldata_LT_table_features = kp.get_parameter_with_keyword('totaldata_LT_table_features')
        totaldata_LT_table_type = kp.get_parameter_with_keyword('totaldata_LT_table_type')
        db.write_one(totaldata_LT_table_name, totaldata_LT_table_features, upload_data, totaldata_LT_table_type)
    # ��ͨOD����ԭʼ�����ϴ����ݿ�
    elif write_type == 'LT_OD':
        LT_OD_table_name = kp.get_parameter_with_keyword('LT_OD_table_name')
        LT_OD_table_features = kp.get_parameter_with_keyword('LT_OD_table_features')
        LT_OD_table_type = kp.get_parameter_with_keyword('LT_OD_table_type')
        db.write_one(LT_OD_table_name, LT_OD_table_features, upload_data, LT_OD_table_type)

    # ���write_typeΪcheckTotal, ����Ի��������ݽ��б���
    elif write_type == 'inoutTime':
        inout_time_table_name = kp.get_parameter_with_keyword('inout_time_table_name')
        inout_time_table_features = kp.get_parameter_with_keyword('inout_time_table_features')
        inout_time_table_type = kp.get_parameter_with_keyword('inout_time_table_type')
        db.write_one(inout_time_table_name, inout_time_table_features, upload_data, inout_time_table_type)

    # ���write_typeΪcheckTotal, ����Ի��������ݽ��б���
    elif write_type == 'gantry_OD':
        gantry_OD_table_name = kp.get_parameter_with_keyword('gantry_OD_table_name')
        gantry_OD_table_features = kp.get_parameter_with_keyword('gantry_OD_table_features')
        gantry_OD_table_type = kp.get_parameter_with_keyword('gantry_OD_table_type')
        db.write_one(gantry_OD_table_name, gantry_OD_table_features, upload_data, gantry_OD_table_type)

    # ���write_typeΪcheckTotal, ����Ի��������ݽ��б���
    elif write_type == 'LT_treated_OD':
        LT_OD_treated_table_name = kp.get_parameter_with_keyword('LT_OD_treated_table_name')
        LT_OD_treated_table_features = kp.get_parameter_with_keyword('LT_OD_treated_table_features')
        LT_OD_treated_table_type = kp.get_parameter_with_keyword('LT_OD_treated_table_type')
        db.write_one(LT_OD_treated_table_name, LT_OD_treated_table_features, upload_data, LT_OD_treated_table_type)

    # portary_LT
    elif write_type == 'portary_LT':
        long_portray_LT_table_name = kp.get_parameter_with_keyword('long_portray_LT_table_name')
        long_portray_LT_table_features = kp.get_parameter_with_keyword('long_portray_LT_table_features')
        long_portray_LT_table_type = kp.get_parameter_with_keyword('long_portray_LT_table_type')
        db.write_one(long_portray_LT_table_name, long_portray_LT_table_features, upload_data,
                     long_portray_LT_table_type)

    # ���write_typeΪcheckTotal, ����Ի��������ݽ��б���
    elif write_type == 'portary_gantry':
        long_portray_gantry_table_name = kp.get_parameter_with_keyword('long_portray_gantry_table_name')
        long_portray_gantry_table_feature = kp.get_parameter_with_keyword('long_portray_gantry_table_feature')
        long_portray_gantry_table_type = kp.get_parameter_with_keyword('long_portray_gantry_table_type')
        db.write_one(long_portray_gantry_table_name, long_portray_gantry_table_feature, upload_data,
                     long_portray_gantry_table_type)


'''
    ����ʱ�䣺2022/05/08
    ���ʱ�䣺2022/05/08
    ���ܣ����и������ϴ����ݿ⡪��δ����
    �޸�ʱ�䣺No.1 2022/6/17, add the charge for ��ʻ��������
'''


def save_data_of_path(write_type, path='', path_type='fold', input_data=None):
    """
    �������ַ�ڵ�����д�����ݿ�
    :param path_type:
    :param input_data: ������������
    :param write_type: д�����ݵ����ͣ�middle����Ϊ�м�����д�룬timePart����һ��ʱ��ϲ����������д�룬checkFeature����������������д�룬checkResult���������д��
    :param path: ��Ҫд����������ڵĵ�ַ
    :return:
    """
    if path_type == 'fold':
        paths = dop.path_of_holder_document(path, True)
    elif path_type == 'files':
        paths = path
    else:
        paths = [path]
    for path in paths:
        # if '20220129' < path.rsplit('/', 1)[1][:-4].replace('-', '') < '20220208' or '20210929' <
        # path.rsplit('/', 1)[1][:-4].replace('-', '') < '20211009':
        #     continue
        if path.rsplit('/', 1)[1][:-4].replace('-', '') <= '20220709':
            continue
        print(path.rsplit('/', 1)[1][:-4].replace('-', ''))
        save_data = []
        with open(path) as f:
            for i, row in enumerate(f):
                row = row.split(',')
                row[-1] = row[-1][:-1]
                if i == 0:  # 2022/6/17 add
                    index = dbf.get_indexs_of_list(row, ['��ʻ��������'])
                else:
                    # �����ͨ��OD���ݽ��д���
                    if write_type == 'LT_OD':
                        list_ls = [i]
                        list_ls.extend(row)
                        save_data.append(list_ls)
                    if write_type == 'inoutTime':
                        save_data.append(row)
                    if write_type == 'gantry_OD':
                        list_ls = [i]
                        list_ls.extend(row)
                        save_data.append(list_ls)
                    if write_type == 'LT_treated_OD':
                        list_ls = [i]
                        list_ls.extend(row[1:])
                        save_data.append(list_ls)
                    if write_type == 'portary_LT':
                        list_ls = row[:-6]
                        list_ls.extend(row[-5:])
                        save_data.append(list_ls)
                    if write_type == 'portary_gantry':
                        save_data.append(row[:-10])
                    if write_type == 'middle':
                        save_data.append(row)
                    elif write_type == 'checkFeature':
                        list_ls = [row[2]]
                        list_ls.extend(row[59:])
                        save_data.append(list_ls)
                    elif write_type == 'checkResult':
                        list_ls = [row[2]]
                        list_ls.extend(row[-17:])
                        save_data.append(list_ls)
                    elif write_type == 'loss':
                        list_ls = row[1:-9]
                        list_ls.append(row[-1])
                        list_ls.extend(row[-9:-1])
                        save_data.append(list_ls)
                    elif write_type == 'whole':
                        list_ls = row[1:-9]
                        list_ls.append(row[21])
                        list_ls.extend(row[-9:])
                        save_data.append(list_ls)
                    elif write_type == 'province':
                        list_ls = row[1:-9]
                        if row[21] != '':
                            list_ls.append(row[21])
                        else:
                            list_ls.append(row[10])
                        list_ls.extend(row[-9:])
                        save_data.append(list_ls)
                    elif write_type == 'part':
                        row.append('part')
                        save_data.append(row)
                    elif write_type == 'checkTotal':
                        save_data.append(row[2:])
                    elif write_type == 'tollinterval':
                        list_ls = [i]
                        list_ls.extend(row)
                        save_data.append(list_ls)
                    elif write_type == 'totaldata':
                        if row[index[0]] != '' and 100 >= float(row[index[0]]) > 10:
                            save_data.append(row)
        if write_type == 'middle' or write_type == 'checkFeature' or write_type == 'checkResult' \
                or write_type == 'checkTotal' or write_type == 'tollinterval' or write_type == 'totaldata' \
                or write_type == 'LT_OD' or write_type == 'inoutTime' or write_type == 'gantry_OD' \
                or write_type == 'LT_treated_OD' or write_type == 'portary_LT' or write_type == 'portary_gantry':
            save_data_to_Mysql(write_type, save_data)
        else:
            save_data_to_Mysql('timePart', save_data)


'''
    ����ʱ�䣺2022/10/11
    ���ʱ�䣺2022/10/11
    ���ܣ��������ݿ�������޸�
    �ؼ��֣�mysql�����ݿ⣬�����޸�
    �޸�ʱ�䣺
'''


def update_data_of_mysql(db_name, table_name, change_feature, change_value, compare_feature, compare_value,
                         compare_sign):
    """
    �������ݿ�������޸�
    :param str db_name: ���ݿ����ƣ����ڴ������ݿ��������
    :param str table_name: ���в��������ݱ�����
    :param list change_feature: ��Ҫ�����޸ĵ��������Ƽ�
    :param list change_value: ��������Ҫ�ĵ�Ŀ��ֵ
    :param list compare_feature: ����Ŀ�����ݵıȽ�������
    :param list compare_value: ����Ŀ�����ݵĸ�����ֵ
    :param list compare_sign: ����Ŀ�����ݵ���������ֵ֮��ıȽϷ���
    :return:
    """
    # ��ȡ���ݿ�����
    # �������ݿ�����
    database_parameter = kp.get_parameter_with_keyword(db_name)
    db = gd.OperationMysql(database_parameter["host"], database_parameter["port"], database_parameter["user"],
                           database_parameter["passwd"], database_parameter["db"])
    # ��������Ҫ�޸ĵ�ֵ
    for k in range(len(change_value)):
        # �����޸����
        # ����ͷ���
        sql_string = 'update ' + table_name + ' set '
        # �����޸����
        for i in range(len(change_feature)):
            if len(change_feature) == 1 or i == (len(change_feature) - 1):
                if type(change_value[k][i]) == str:
                    sql_string += change_feature[i] + '="' + change_value[k][i] + '"'
                else:
                    sql_string += change_feature[i] + '=' + str(change_value[k][i])
            else:
                if type(change_value[k][i]) == str:
                    sql_string += change_feature[i] + '="' + change_value[k][i] + '",'
                else:
                    sql_string += change_feature[i] + '=' + str(change_value[k][i]) + ','

        # ����ƥ�����
        where_string = ''
        for i in range(len(compare_feature[k])):
            if type(compare_value[k][i]) == str:
                where_string += compare_feature[k][i] + ' ' + compare_sign[k][i] + ' "' + compare_value[k][i] + '"'
            elif type(compare_value[k][i]) == list:
                for j in range(len(compare_value[k][i])):
                    if type(compare_value[k][i][j]) == str:
                        if j == 0:
                            where_string += compare_feature[k][i] + ' ' + compare_sign[k][i] + ' ("' + \
                                            compare_value[k][i][j] + '"'
                        else:
                            where_string += ', "' + str(compare_value[k][i][j]) + '"'
                    else:
                        if j == 0:
                            where_string += compare_feature[k][i] + ' ' + compare_sign[k][i] + ' (' + str(
                                compare_value[k][i][j])
                        else:
                            where_string += ', ' + str(compare_value[k][i][j])
                where_string += ')'
            else:
                where_string += compare_feature[k][i] + ' ' + compare_sign[k][i] + ' ' + str(compare_value[k][i])
            if i < (len(compare_feature[k]) - 1):
                where_string += ' and '
            else:
                pass
        if where_string == '':
            pass
        else:
            sql_string = " where " + where_string

        if k < (len(change_value) - 1):
            # �޸�����
            db.update_data(sql_string, False)
        else:
            db.update_data(sql_string, True)


'''
    ����ʱ�䣺2022/09/02
    ���ʱ�䣺2022/09/06
    ���ܣ��������ݿ�����ݻ�ȡ
    �ؼ��֣�mysql�����ݿ⣬���ݻ�ȡ
    �޸�ʱ�䣺No.1 2022/10/31, add the new parameter 'addSQL'
'''


def load_data_from_mysql(db_name, table_name, feature_name, feature_value, compare_sign, get_feature='all',
                         return_type='list', addSQL=''):
    """
    �������ݿ�����ݻ�ȡ
    :param addSQL:
    :param str db_name: database name
    :param str return_type: ���ݿ��ȡ�����ݵķ�����ʽ
    :param str/list get_feature: ��Ҫ��ȡ��������
    :param str table_name: ���в�ѯ�����ݱ�����
    :param list feature_name: �����������ƣ���������
    :param list feature_value: �����Ա���ֵ����������
    :param list compare_sign: �����Աȷ�ʽ���ţ���������
    :return:
    """
    # ���ɲ�ѯ���
    # ���ɻ�ȡ�ֶε����
    if get_feature == 'all':
        feature_string = "*"
    else:
        feature_string = ''
        for i in range(len(get_feature)):
            if i < (len(get_feature) - 1):
                feature_string += get_feature[i] + ","
            else:
                feature_string += get_feature[i]
    # ���ɲ�ѯwhere������
    where_string = ''
    for i in range(len(feature_name)):
        if i < (len(feature_name) - 1):
            if type(feature_value[i]) == str:
                where_string += feature_name[i] + ' ' + compare_sign[i] + ' "' + feature_value[i] + '" and '
            elif type(feature_value[i]) == list:
                for j in range(len(feature_value[i])):
                    if j == 0:
                        where_string += feature_name[i] + ' ' + compare_sign[i] + ' (' + str(feature_value[i][j])
                    else:
                        where_string += ', ' + str(feature_value[i][j])
                where_string += ') and '
        else:
            if type(feature_value[i]) == str:
                where_string += feature_name[i] + ' ' + compare_sign[i] + ' "' + feature_value[i] + '"'
            elif type(feature_value[i]) == list:
                for j in range(len(feature_value[i])):
                    if j == 0:
                        where_string += feature_name[i] + ' ' + compare_sign[i] + ' ("' + str(feature_value[i][j]) + '"'
                    else:
                        where_string += ', "' + str(feature_value[i][j]) + '"'
                where_string += ')'
    if where_string == '':
        sql_string = "SELECT " + feature_string + " from " + table_name
    else:
        sql_string = "SELECT " + feature_string + " from " + table_name + " where " + where_string

    # 2022/10/31 update
    if addSQL != '':
        sql_string += ' and ' + addSQL

    # ��ȡ���ݿ�����
    # �������ݿ�����
    database_parameter = kp.get_parameter_with_keyword(db_name)
    db = gd.OperationMysql(database_parameter["host"], database_parameter["port"], database_parameter["user"],
                           database_parameter["passwd"], database_parameter["db"])

    # ��ѯ����
    output_data = db.load_data(sql_string)

    # ������ת��Ϊ������������
    if return_type == 'list':
        return output_data


'''
    ����ʱ�䣺2022/10/08
    ���ʱ�䣺2022/10/08
    ���ܣ��������ݼ�������д��
    �ؼ��֣�mysql�����ݿ⣬����д��
    �޸�ʱ�䣺
'''


def write_data_to_mysql(upload_data, db_name, table_name, feature_name, feature_type, mysql_type):
    """

    :param list upload_data: ��Ҫ�ϴ�������
    :param str db_name: ��ȡ���ݿ�������Ϣ�Ĺؼ���
    :param str table_name: ��ȡ���ݱ����ƵĹؼ���
    :param str feature_name: ��ȡ���ݱ���ֶ����ƵĹؼ���
    :param str feature_type: ��ȡ���ݱ���ֶ����͵Ĺؼ���
    :param str mysql_type: �ϴ���ʽ
    :return:
    """
    # ��ȡ��������ݿ��������Ӳ���
    database_parameter = kp.get_parameter_with_keyword(db_name)
    # �����Ӳ������룬�õ�mysql����
    db = gd.OperationMysql(database_parameter["host"], database_parameter["port"], database_parameter["user"],
                           database_parameter["passwd"], database_parameter["db"])
    # �õ��ϴ���Ŀ�����ݱ�����
    long_portray_gantry_table_name = kp.get_parameter_with_keyword(table_name)
    # �õ��ϴ��ĸ������ֶ�����
    long_portray_gantry_table_feature = kp.get_parameter_with_keyword(feature_name)
    # �õ��ϴ��ĸ���������
    long_portray_gantry_table_type = kp.get_parameter_with_keyword(feature_type)
    # ���������ϴ�
    db.write_one(long_portray_gantry_table_name, long_portray_gantry_table_feature, upload_data,
                 long_portray_gantry_table_type, data_type=mysql_type, save_num=500)


if __name__ == '__main__':
    data = pd.read_csv('./Data_Origin/test_station.csv')
    # data = data.sort_values('time', ascending=False)
    # data = data
    # data = data.drop_duplicates(subset=['INTERVAL_ID', 'TIME_POINT'], keep='first')
    data = data.fillna(0)
    write_data_to_mysql(data.values, 'overspeed_mysql', 'station_flow_base_name', 'station_flow_base_features',
                        'station_flow_base_features_type', 'csv')
