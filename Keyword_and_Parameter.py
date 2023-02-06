"""
针对各业务的关键字集合
文档创建时间：2021/12/22
文档修改时间：2022/02/18，增加wash_enter_from_exit的字段，用于将原始数据串行造成的获取错误去除
           2022/3/21, exit data add the 'ENWEIGHT', 'EXWEIGHT'
           2022/10/28,
"""


def get_parameter_with_keyword(keys):
    """
    根据提供的关键字，获取对应的参数名称集合
    思路：将关键字和对应参数以字典形式保存，然后直接通过关键字获取参数内容
    :param keys:关键字
    :return:
    """
    config = {}
    with open('./upload/config_test.txt') as f:
        for i, row in enumerate(f):
            if row == '':
                break
            else:
                row = row.split(':')
                row[-1] = row[-1][:-1]
                if row[0] == 'port':
                    config[row[0]] = int(row[1])
                else:
                    config[row[0]] = row[1]

    parameter_disc = {
        # 门架数据清洗各关键字和对应参数
        # 2022/2/23更新，wash_gantry去掉了GANTRYTYPE、OBUVEHICLEPLATE、CPUVEHICLEPLATE，增加了ENTOLLSTATIONHEX
        'wash_gantry': ['BIZID', 'PASSID', 'VEHICLEPLATE', 'IDENTIFYVEHICLEID', 'ENTRYSTATION', 'EXITSTATION',
                        'FEE', 'GANTRYID', 'TRANSTIME', 'ENTIME', 'MEDIATYPE', 'VEHICLETYPE', 'VEHICLECLASS',
                        'ENTOLLSTATIONNAME', 'ENWEIGHT', 'VEHICLETYPE', 'VEHICLECLASS', 'ENTOLLSTATIONHEX',
                        'TRADERESULT'],
        'wash_gantry_path': ['BIZID', 'PASSID', 'FEE', 'GANTRYID', 'TRANSTIME', 'ENTIME', 'TOLLINTERVALID',
                             'VEHICLETYPE', 'ENTOLLSTATIONHEX'],  # 2022/8/8 change, add the VEHICLETYPE
        # 出口收费数据清洗各关键字和对应参数
        # 2022/2/23更新，增加了OBUVEHICLETYPE/EXITFEETYPE/ENAXLECOUNT/EXAXLECOUNT/OBUSN/ETCCARDID/ENSTATIONHEX
        # 增加了入口信息，ENVEHICLEID、ENIDENTIFYVEHICLEID、ENTRYSTATION、，去掉OBUVEHICLEID、CARDVEHICLEID
        # 2022/3/8 changed, take the FEE to TOTALTOLL
        # 2022/3/21 add, add the 'ENWEIGHT', 'EXWEIGHT'
        'wash_exit': ['EXDEALID', 'PASSID', 'ENVEHICLEID', 'ENIDENTIFYVEHICLEID', 'ENTRYSTATION', 'EXVEHICLEID',
                      'EXIDENTIFYVEHICLEID', 'EXITSTATION', 'TOTALTOLL', 'GRANTRYTRADEID', 'EXTIME', 'ENTIME',
                      'MEDIATYPE', 'ENVEHICLETYPE', 'ENVEHICLECLASS', 'EXVEHICLETYPE', 'EXVEHICLECLASS',
                      'ENTOLLSTATIONNAME', 'OBUVEHICLETYPE', 'ENAXLECOUNT', 'EXAXLECOUNT', 'OBUSN', 'ETCCARDID',
                      'EXITFEETYPE', 'ENSTATIONHEX', 'OBUVEHICLEID', 'CARDVEHICLEID',
                      'ENWEIGHT', 'EXWEIGHT', 'ENDEALID', 'EXDEALID', 'VEHICLESIGNID', 'PICBATCH', 'PICNO',
                      'VEHICLETAILSIGNID', 'TAILPICBATCH', 'TAILPICNO', 'RECORDTYPE', 'PAYTYPE', 'PAYKIND',
                      'DEALSTATUS'],
        # 2022/2/23更新，增加了OBUVEHICLETYPE/EXITFEETYPE/ENAXLECOUNT/EXAXLECOUNT/OBUSN/ETCCARDID/ENSTATIONHEX，去除了OBUVEHICLEPLATE、CPUVEHICLEPLATE
        # 2022/3/21 add, add the 'ENWEIGHT', 'EXWEIGHT'
        'wash_exit_name': ['BIZID', 'PASSID', 'ENVEHICLEPLATE', 'ENIDENTIFYVEHICLEID', 'ENTRYSTATION', 'EXVEHICLEPLATE',
                           'EXIDENTIFYVEHICLEID', 'EXITSTATION', 'FEE', 'EXGANTRYID', 'EXTIME', 'ENTIME', 'MEDIATYPE',
                           'ENVEHICLETYPE', 'ENVEHICLECLASS', 'EXVEHICLETYPE', 'EXVEHICLECLASS', 'ENTOLLSTATIONNAME',
                           'OBUVEHICLETYPE', 'ENAXLECOUNT', 'EXAXLECOUNT', 'OBUSN', 'ETCCARDID', 'EXITFEETYPE',
                           'ENSTATIONHEX', 'OBUVEHICLEPLATE', 'CPUVEHICLEPLATE',
                           'ENWEIGHT', 'EXWEIGHT', 'ENSTATIONFX', 'EXSTATIONFX'],
        # 2022/3/8 changed, take the FEE to TOTALTOLL
        'wash_exit_path': ['EXDEALID', 'PASSID', 'TOTALTOLL', 'GRANTRYTRADEID', 'EXTIME', 'ENTIME', 'VEHICLESIGNID',
                           'PICBATCH', 'PICNO', 'VEHICLETAILSIGNID', 'TAILPICBATCH', 'TAILPICNO', 'RECORDTYPE',
                           'PAYTYPE', 'PAYKIND', 'DEALSTATUS'],
        'wash_exit_path_name': ['BIZID', 'PASSID', 'FEE', 'GANTRYID', 'TRANSTIME', 'ENTIME', 'TOLLINTERVALID',
                                'length'],
        # 入口收费数据清洗各关键字和对应参数
        'wash_enter': ['ENDEALID', 'PASSID', 'ENVEHICLEID', 'ENIDENTIFYVEHICLEID', 'ENTRYSTATION', 'FEE',
                       'ENTIME', 'MEDIATYPE', 'ENVEHICLETYPE', 'GRANTRYTRADEID', 'ENVEHICLECLASS', 'ENTOLLSTATIONNAME',
                       'ENSTATIONHEX', 'ENWEIGHT', 'ENDEALID', 'VEHICLESIGNID', 'PICBATCH', 'PICNO',
                       'VEHICLETAILSIGNID', 'TAILPICBATCH', 'TAILPICNO', 'RECORDTYPE', 'PAYTYPE', 'PAYKIND',
                       'DEALSTATUS'],
        # 2022/2/23更新，去除了OBUVEHICLEPLATE、CPUVEHICLEPLATE
        'wash_enter_name': ['BIZID', 'PASSID', 'VEHICLEPLATE', 'IDENTIFYVEHICLEID', 'ENTRYSTATION',
                            'FEE', 'ENTIME', 'MEDIATYPE', 'VEHICLETYPE', 'GANTRYID', 'VEHICLECLASS',
                            'ENTOLLSTATIONNAME', 'ENSTATIONHEX', 'ENWEIGHT', 'ENSTATIONFX'],
        'wash_enter_path': ['ENDEALID', 'PASSID', 'FEE', 'GRANTRYTRADEID', 'ENTIME', 'ENTIME', 'VEHICLESIGNID',
                            'PICBATCH', 'PICNO', 'VEHICLETAILSIGNID', 'TAILPICBATCH', 'TAILPICNO', 'RECORDTYPE',
                            'PAYTYPE', 'PAYKIND', 'DEALSTATUS'],
        'wash_enter_path_name': ['BIZID', 'PASSID', 'FEE', 'GANTRYID', 'TRANSTIME', 'ENTIME', 'TOLLINTERVALID',
                                 'length'],

        # GPS数据清洗各关键字和对应参数
        # 'wash_GPS': ['VEHICLE_NO', 'VEHICLE_COLOR', 'DATE_TIME', 'LON', 'LAT'],
        'wash_GPS': ['CARNUM', 'CARCOLOR', 'TIMESTAMPSTR', 'LON', 'LAT'],

        # GPS匹配所需的车辆收费数据的字段
        'GPS_vehicle_features': ['PASSID', '车牌(全)', '门架路径', '门架时间串', '类型', '车型', '出口车型', '入口车型'],

        # the score of abnormal is high车辆GPS匹配所需的字段
        'GPS_highAbnormalScore_vehicle_features': ['PASSID', '车牌(全)', '门架路径', '门架时间串', '类型', '行驶风险评分'],

        # GPS匹配所需的LT车辆收费数据的字段
        'GPS_LT_vehicle_features': ['PASSID', '车牌(全)', '门架路径', '门架时间串', '类型', '不合格原因', '入口车型', '查验结果'],

        # 从出口信息中提取入口信息的关键字和对应参数
        'wash_enter_from_exit': ['PASSID', 'ENVEHICLEID', 'ENTRYSTATION', 'GRANTRYTRADEID',
                                 'ENTIME', 'ENVEHICLETYPE', 'ENVEHICLECLASS', 'EXWEIGHT', 'ENWEIGHT',
                                 'ENTOLLSTATIONNAME', 'VEHICLESIGNID', 'PICBATCH', 'PICNO', 'VEHICLETAILSIGNID',
                                 'TAILPICBATCH', 'TAILPICNO', 'ENPROVID', 'RECORDTYPE', 'PAYTYPE', 'PAYKIND',
                                 'DEALSTATUS'],
        'wash_enter_from_exit_name': ['PASSID', 'VEHICLEPLATE', 'ENTRYSTATION', 'GANTRYID', 'TRANSTIME', 'VEHICLETYPE',
                                      'VEHICLECLASS', 'EXWEIGHT', 'ENWEIGHT', 'ENTOLLSTATIONNAME', 'STATIONFX'],
        # 绿通清洗内容
        'LT_province': ["北京市", "天津市", "广东省", "河北省", "山西省", "广西壮族自治区", "贵州省", "黑龙江省", "内蒙古自治区",
                        "辽宁省", "吉林省", "湖南省", "江苏省", "上海市", "青海省", "安徽省", "浙江省", "福建省", "江西省", "山东省",
                        "河南省", "湖北省", "重庆市", "四川省", "云南省", "陕西省", "甘肃省", "新疆维吾尔自治区", "宁夏回族自治区"],

        'LT_kind': ["新鲜蔬菜", "白菜类", "大白菜", "普通白菜(油菜、小青菜)", "甘蓝类", "菜花", "芥蓝", "西兰花", "结球甘蓝", "菜薹", "根菜类",
                    "萝卜", "胡萝卜", "芜菁", "绿叶菜类", "芹菜", "菠菜", "莴笋", "生菜", "空心菜", "香菜", "茼蒿", "茴香", "苋菜", "木耳菜",
                    "葱蒜类", "洋葱", "大葱", "香葱", "蒜苗", "蒜苔", "韭菜", "大蒜", "茄果类", "茄子", "青椒", "辣椒", "西红柿", "豆类", "扁豆",
                    "荚豆", "豇豆", "豌豆", "四季豆", "毛豆", "蚕豆", "豆芽", "豌豆苗", "四棱豆", "瓜类", "黄瓜", "丝瓜", "冬瓜", "西葫芦", "苦瓜",
                    "南瓜", "佛手瓜", "蛇瓜", "节瓜", "瓠瓜", "薯芋类", "马铃薯", "甘薯(红薯、白薯、山药、芋头)", "鲜玉米", "鲜花生", "生姜",
                    "水生蔬菜", "莲藕", "荸荠", "水芹", "茭白", "新鲜食用菌", "平菇", "金针菇", "滑菇", "蘑菇", "木耳(不含干木耳)",
                    "多年生和杂类蔬菜", "竹笋", "芦笋", "金针菜(黄花菜)", "香椿", "新鲜水果", "仁果类", "苹果", "梨", "海棠", "山楂", "核果类",
                    "桃", "李", "杏", "杨梅", "樱桃", "浆果类", "葡萄", "提子", "草莓", "猕猴桃", "石榴", "桑葚", "无花果", "柑橘类", "橙", "桔",
                    "柑", "柚", "柠檬", "热带及亚热带水果", "香蕉", "菠萝", "龙眼", "荔枝", "橄榄", "枇杷", "椰子", "芒果", "杨桃", "木瓜",
                    "火龙果", "番石榴", "莲雾", "柿枣类", "枣", "柿子", "瓜果类", "西瓜", "甜瓜", "哈密瓜", "香瓜", "伊丽莎白瓜", "华莱士瓜",
                    "鲜活水产品", "鱼虾贝蟹", "鱼类", "虾类", "贝类", "蟹类", "其它水产品", "海带", "紫菜", "海蜇", "海参", "活的畜禽", "猪",
                    "仔猪", "其他", "蜜蜂(转地放蜂)", "新鲜的肉、蛋、奶", "鲜肉蛋奶", "新鲜的鸡蛋", "鸭蛋", "鹅蛋", "鹌鹑蛋",
                    "新鲜的家禽肉和家畜肉", "新鲜奶", "联合收割机", "插秧机"],

        # 'LT_create_type': ['开放式', '敞篷式', '平板式敞篷', '栅栏式敞篷', '封闭式', '罐式', '水箱式', '封闭箱式', '帆布包裹式'],
        'LT_create_type': ['平板式敞篷', '栅栏式敞篷', '罐式', '水箱式', '封闭箱式', '帆布包裹式'],

        'LT_feature': ['绿通跨省次数', '跨省比', '是否异常上下高速', '作弊次数', '作弊时间'],

        # 绿通大数据分析所需字段
        'LT_analysis_feature': ['入口地', '出口地', '入口时间', '查验时间', '绿通品种', '查验结果', '车型', '厢型'],
        # 绿通OD所需字段
        'LT_OD_feature': ['车牌(全)', '所在市1', '所在市2', 'number'],

        # gantryOD所需字段
        'whole_OD_feature': ['车牌(全)', '收费站1', '收费站2', '所在市1', '所在市2', 'number'],

        # 绿通车型、厢型、品种与载重关系分析所需字段
        'LT_weight_feature': ['PASSID', '查验时间', '绿通品种', '查验结果', '车型', '厢型', '重量'],

        # 长期综合画像的所需字段
        'total_portary_feature': ['出口车种', '出口门架时间', '入口重量', '出口重量', '出口计费方式', '本省入口站id', '本省出口站id', '类型', 'PASSID',
                                  '车牌(全)', '出口车型'],
        # 长期综合画像loss的所需字段
        'total_loss_portary_feature': ['类型', '车牌(全)'],

        # gantry长期OD & time所需字段
        'OD_time_portary_feature': ['车牌(全)', '本省入口站id', '本省出口站id', '类型', '入口时间', '出口时间', 'middle_type'],

        # KT长期OD所需字段
        'LT_OD_portary_feature': ['车牌(全)', '入口站ID', '出口站ID', '入口收费站', '出口收费站', '入口地', '出口地', '入口市', '出口市', '入口省',
                                  '出口省'],

        # 基于省内稽查结果数据的车辆长期画像
        'result_portary_feature': ['车牌(全)', 'OBU设备号', 'ETC卡号', '出口车型', '出口车种', '门架筛查代码',
                                   '出入口匹配（车牌）', '出入口匹配（车牌颜色）', 'OBU车牌匹配', 'ETC卡车牌匹配', '门架数占比',
                                   '出口通行介质', '出入口匹配（车型）_排除拖挂', '是否门架行驶超时', '行驶时间高于3Days', '路径类型异常代码',
                                   '行驶时间异常代码', '费用对比异常代码', '出口轴数匹配（出口车型）', '入口通行介质', '类型', '入口车型', '入口车种'],
        'loss_portary_feature': ['车牌(全)', 'OBU设备号', 'ETC卡号', '出口车型', '出口车种', '出口通行介质', '入口通行介质',
                                 '类型', '入口车型', '入口车种'],

        # 绿通车辆画像的所需字段
        'LT_one_portary_feature': ['厢型', '查验时间', '班组标号', '验货人员编号', '复核人员编号', '查验结果',
                                   '不合格原因', '入口收费站', '入口地', '入口市', '入口省',
                                   '出口收费站', '出口地', '出口市', '出口省', '绿通品种', '入口时间', '入口地输出概率',
                                   '出口地输入概率', '车型与品种匹配度', '出入地绿通运输概率', '是否载重异常', '是否异常上下高速'],

        # 各日数据之间的PASSID去重所需的字段
        'drop_repeat_passid': ['PASSID', '出口时间', '出口门架时间', '收费单元路径', '门架路径', '门架时间串', '门架费用', '总里程', 'if_haveCard',
                               'middle_type', '类型', '门架数', '门架费用串', '门架类型串', '本省入口ID', '本省入口站id', '入口收费站名称', '本省入口时间',
                               '本省出口ID', '本省出口站id', '出口收费站名称', '本省出口时间', '是否端口为省界', '出口车牌(全)', '入口车牌(全)', '入口时间'],

        # 收费站各方向上下站流量计算所需的字段
        'compute_station_num': ['收费单元路径', '本省入口站id', '入口HEX码', '入口车型', '入口时间', '本省入口时间', '本省出口站id', '出口车型', '出口时间',
                                '本省出口时间'],
        'compute_station_num_middle': ['收费单元路径', '入口ID', '入口HEX码', '入口车型', '入口时间', '入口门架时间', '出口ID', '出口车型', '出口时间',
                                       '出口门架时间'],

        # 绿通车辆长期画像的所需字段

        'abnormal_portary_feature': ['if_haveCard', '是否采用最小费额计费', '门架筛查代码', '出入口匹配（车牌）', '出入口匹配（车牌颜色）', 'OBU车牌匹配',
                                     'ETC卡车牌匹配',
                                     '路径是否完整', '出口轴数匹配（出口车型）', 'obu车型匹配（出口车型）', '行驶时间高于3Days', '出入口匹配（车型）_排除拖挂',
                                     '行驶时间异常代码', '费用对比异常代码', '路径类型异常代码'],

        'time_portary_feature': ['门架筛查代码', '出入口匹配（车牌）', '出入口匹配（车牌颜色）', 'OBU车牌匹配', 'ETC卡车牌匹配',
                                 '路径是否完整', '出口轴数匹配（出口车型）', 'obu车型匹配（出口车型）', '行驶时间高于3Days', '出入口匹配（车型）_排除拖挂',
                                 '行驶时间异常代码', '费用对比异常代码', '路径类型异常代码', '类型'],

        '门架筛查代码': ['X', 'Y'],
        '出入口匹配（车牌）': ['G'],
        '出入口匹配（车牌颜色）': ['I'],
        'OBU车牌匹配': ['L'],
        'ETC卡车牌匹配': ['S'],
        '路径是否完整': ['2.0'],
        '出口轴数匹配（出口车型）': ['U'],
        'obu车型匹配（出口车型）': ['V'],
        '行驶时间高于3Days': ['W'],
        '出入口匹配（车型）_排除拖挂': ['M'],
        '行驶时间异常代码': ['T', 'P'],
        '费用对比异常代码': ['Q'],
        '路径类型异常代码': ['C', 'D', 'E'],
        '类型': ['only_in', 'only_out', 'none'],
        '出口缺失频次': 1,
        '入口缺失频次': 1,
        'OBU设备有无更换记录': 10,
        'ETC卡有无更换记录': 10,
        '出入口缺失频次': 1,
        '是否有军车变车种情况': 10,
        '是否一车多型': 5,
        '出入口重量不一致频次': 0.1,

        'time_portary_name': ['是否发生过绿通作弊', '是否发生过偷逃费', '路径缺失X频次', '路径缺失Y频次', '出入口车牌不匹配频次',
                              '出入口车牌颜色不匹配频次',
                              'OBU设备内车牌不匹配次数', 'ETC卡内车牌不匹配次数', '全程无门架记录频次', '出口轴数大于计费车型频次',
                              'obu车型不匹配频次', '行驶时长超过3天的频次', '出入口车型不一致频次', '行驶时间异常T频次',
                              '行驶时间异常P频次', '费用异常频次', 'U型行驶频次', 'J型行驶频次', '往复型行驶频次', '出口缺失频次',
                              '入口缺失频次', '出入口缺失频次'],

        # XiBao interval
        # 'XB_gantrys': ["G003061003000120", "G003061003000210"],
        'XB_gantrys': ["G003061003000120", "G003061003000210", "G003061003000110", "G003061003000310",
                       "G003061003000220",
                       "G003061003000410", "G003061003000320", "G003061003000510", "G003061003000420",
                       "G003061003000520",
                       "G003061003000710", "G003061003000720", "G003061003000810", "G003061003000820",
                       "G003061003001310",
                       "G003061003000610", "G003061003000620", "G003061003000910", "G003061003000920",
                       "G003061003001010",
                       "G003061003001020", "G003061003001120", "G003061003001110", "G003061003001220",
                       "G003061003001210",
                       "G003061003001320", "G003061003001410", "G003061003001420", "G003061003001510",
                       "G003061003001520",
                       "G003061003001610", "G003061003001620", "G003061003001710", "G003061003001720"],
        # XiBao station
        'XB_station': ["G0030610030020", "G0030610030030", "G0030610030040", "G0030610030050",
                       "G0030610030060", "G0030610030070", "G0030610030080", "G0030610030090", "G0030610030100",
                       "G0030610030110", "G0030610030120", "G0030610030130", "G0030610030140"],

        'vehicle_type': ['1', '2', '3', '4', '11', '12', '13', '14', '15', '16'],

        # 各车型对应的车身长范围
        'vehicle_length': {1: [6, 1], 2: [7, 1], 3: [11, 1], 4: [15, 2], 11: [6, 1], 12: [8, 1], 13: [12, 1],
                           14: [12, 1], 15: [12, 2], 16: [20, 2], 21: [6, 1], 22: [8, 1], 23: [9, 1], 24: [10, 1],
                           25: [12, 2], 26: [24, 3]},

        # 各车型对应的车身长范围
        'gap_length': {1: [3, 0.3], 2: [3, 0.3], 3: [3, 0.3], 4: [3, 0.3], 11: [3, 0.5], 12: [3, 0.5],
                       13: [3, 0.5], 14: [3, 0.5], 15: [3, 0.5], 16: [3, 0.5], 21: [3, 0.5], 22: [3, 0.5], 23: [3, 0.5],
                       24: [3, 0.5], 25: [3, 0.5], 26: [3, 0.5]},

        # 各高速段的起止门架ID
        'start_end_interval': {'XH': ['G0005610030004', 'G0005610050005']},

        # 保存各数据库参数内容
        # 部中心数据库
        'LT_department_mysql': {'host': '60.205.149.33', 'port': 18607, 'user': 'cx', 'passwd': 'Lvtongcx2019',
                                'db': 'freeway_appointment'},

        # 陕西绿通车
        'LT_province_mysql': {'host': '60.205.149.33', 'port': 3306, 'user': 'cx', 'passwd': 'cd2017',
                              'db': 'lvtongche'},

        # 收费中心
        'vehicle_charge_mysql': {'host': '172.16.1.54', 'port': 3306, 'user': 'zhidu', 'passwd': 'sjfx2018',
                                 'db': 'road_infrastructure'},

        # 测试数据库
        'vehicle_test_mysql': {'host': '192.168.0.95', 'port': 3306, 'user': 'root', 'passwd': '123',
                               'db': 'freeway_appointment'},

        # 全路网稽查数据库
        'vehicle_check_mysql': {'host': '192.168.0.182', 'port': 3306, 'user': 'root', 'passwd': '123456',
                                'db': 'vehicle_check'},

        # XB数据库
        # 'overspeed_mysql': {'host': '192.168.17.16', 'port': 3306, 'user': 'root', 'passwd': '123',
        #                     'db': 'overspeed'},
        'overspeed_mysql': config,
        # 'overspeed_mysql': {'host': '192.168.0.170', 'port': 3306, 'user': 'overspeed', 'passwd': 'mkdir@2021',
        #                     'db': 'overspeed'},

        # 数据库的数据存储位置
        # 一天数据的保存位置
        # 最新一天的绿通数据存储位置
        'LT_oneDay_save_path': '../Origin_Data/oneDay/latest_data/LT/',
        # 最新一天的绿通数据备份存储位置
        'LT_oneDay_back_path': '../Origin_Data/oneDay/back_data/LT/',

        # 最新一天的门架收费数据存储位置
        'gantry_oneDay_save_path': '../Origin_Data/oneDay/latest_data/gantry/',
        # 最新一天的门架收费数据备份存储位置
        'gantry_oneDay_back_path': '../Origin_Data/oneDay/back_data/gantry/',

        # 最新一天的省界门架收费数据存储位置
        'gantry_oneDay_save_path_pro': '../Origin_Data/oneDay/latest_data/gantry_pro/',
        # 最新一天的省界门架收费数据备份存储位置
        'gantry_oneDay_back_path_pro': '../Origin_Data/oneDay/back_data/gantry_pro/',

        # 最新一天的入口收费数据存储位置
        'in_oneDay_save_path': '../Origin_Data/oneDay/latest_data/entry/',
        # 最新一天的入口收费数据备份存储位置
        'in_oneDay_back_path': '../Origin_Data/oneDay/back_data/entry/',

        # 最新一天的出口收费数据存储位置
        'out_oneDay_save_path': '../Origin_Data/oneDay/latest_data/exit/',
        # 最新一天的出口收费数据备份存储位置
        'out_oneDay_back_path': '../Origin_Data/oneDay/back_data/exit/',

        # 最新一天的牌识收费数据存储位置
        'iden_oneDay_save_path': '../Origin_Data/oneDay/latest_data/identify/',
        # 最新一天的牌识收费数据备份存储位置
        'iden_oneDay_back_path': '../Origin_Data/oneDay/back_data/identify/',

        # 大量数据的存储位置

        # GPS数据的各存储位置
        # GPS原始数据的存储位置
        'GPS_origin_data_path': '../GPS/GPS_HC/wait_for_wash',
        # GPS清洗后数据的存储位置
        'GPS_wash_data_path': './2.GPS_Data_12T_FreightTrain/November/Origin_Data/',
        # GPS离散化后数据的存储位置
        'GPS_separate_data_path': './2.GPS_Data_12T_FreightTrain/November/GPS_for_Vehicle/',
        # GPS匹配后数据的存储位置
        'GPS_vehicle_data_path': './2.GPS_Vehicle_Data/',
        # 新GPS匹配后数据的存储位置
        'GPS_vehicle_data_path_new': './2.GPS_Vehicle_Data_New/',
        # 新GPS的存放主文件夹地址
        'GPS_separate_data_path_new': '../GPS/track/',

        # 清洗后的数据存储位置
        # 最新一天的绿通数据存储位置
        'LT_oneDay_wash_save_path': './2.oneDay/latest_data/LT/',
        # 最新一天的绿通数据备份存储位置
        'LT_oneDay_wash_back_path': './2.oneDay/back_data/LT/',
        # 近一周的绿通合并数据存储位置
        'LT_week_wash_path': './2.oneDay/last_week_data/LT/',

        # 最新一天的门架收费清洗数据存储位置
        'gantry_oneDay_wash_save_path': './2.oneDay/latest_data/gantry/',
        # 最新一天的门架收费清洗数据备份存储位置
        'gantry_oneDay_wash_back_path': './2.oneDay/back_data/gantry/',
        # 最新一天的门架收费清洗数据 基础信息存储位置
        'gantry_oneDay_wash': './2.oneDay/latest_data/gantry/gantry',
        # 最新一天的门架收费清洗数据 路径信息存储位置
        'gantry_oneDay_wash_path': './2.oneDay/latest_data/gantry/gantry_path',

        # 最新一天的省界门架收费清洗数据存储位置
        'gantry_oneDay_wash_save_path_pro': './2.oneDay/latest_data/gantry_pro/',
        # 最新一天的省界门架收费清洗数据备份存储位置
        'gantry_oneDay_wash_back_path_pro': './2.oneDay/back_data/gantry_pro/',
        # 最新一天的省界门架收费清洗数据 基础信息存储位置
        'gantry_pro_oneDay_wash': './2.oneDay/latest_data/gantry_pro/gantry',
        # 最新一天的省界门架收费清洗数据 路径信息存储位置
        'gantry_pro_oneDay_wash_path': './2.oneDay/latest_data/gantry_pro/gantry_path',

        # 最新一天的入口收费清洗数据存储位置
        'in_oneDay_wash_save_path': './2.oneDay/latest_data/enter/',
        # 最新一天的入口收费清洗数据备份存储位置
        'in_oneDay_wash_back_path': './2.oneDay/back_data/enter/',
        # 最新一天的入口收费清洗数据 基础信息存储位置
        'in_pro_oneDay_wash': './2.oneDay/latest_data/enter/Enter',
        # 最新一天的入口收费清洗数据 路径信息存储位置
        'in_pro_oneDay_wash_path': './2.oneDay/latest_data/enter/Enter_path',

        # 最新一天的出口收费清洗数据存储位置
        'out_oneDay_wash_save_path': './2.oneDay/latest_data/exit/',
        # 最新一天的出口收费清洗数据备份存储位置
        'out_oneDay_wash_back_path': './2.oneDay/back_data/exit/',
        # 最新一天的出口收费清洗数据 基础信息存储位置
        'out_pro_oneDay_wash': './2.oneDay/latest_data/exit/exit',
        # 最新一天的出口收费清洗数据 路径信息存储位置
        'out_pro_oneDay_wash_path': './2.oneDay/latest_data/exit/exit_path',

        # 最新一天的牌识收费清洗数据存储位置
        'iden_oneDay_wash_save_path': './2.oneDay/latest_data/identify/',
        # 最新一天的牌识收费清洗数据备份存储位置
        'iden_oneDay_wash_back_path': './2.oneDay/back_data/identify/',

        # 中间数据的保存地址
        # 最新一天的中间数据存储位置
        'middle_oneDay_save_path': './2.oneDay/latest_data/Middle_Data/',
        # 最新一天的中间数据备份存储位置
        'middle_oneDay_back_path': './2.oneDay/back_data/Middle_Data/',
        # 多日的中间数据备份存储位置
        'middle_manyDay_back_path': './2.Middle_Data/',

        # 长期画像数据存储位置
        # many days长期画像的车辆各OD次数统计数据的保存地址
        'whole_OD_num': './4.poratry_data/whole_OD_num_data.csv',
        # one day长期画像的车辆各OD次数统计数据的保存地址
        'back_whole_OD_num': './4.poratry_data/back_whole_OD_num/',
        # lastly day长期画像的车辆各OD次数统计数据的保存地址
        'last_whole_OD_num': './4.poratry_data/last_whole_OD_num/',

        # many days长期画像上下高速时段次数的统计数据保存地址
        'whole_time_num': './4.poratry_data/whole_time_num_data.csv',
        # one day长期画像上下高速时段次数的统计数据保存地址
        'back_whole_time_num': './4.poratry_data/back_whole_time_num/',
        # lastly day长期画像上下高速时段次数的统计数据保存地址
        'last_whole_time_num': './4.poratry_data/last_whole_time_num/',

        # many days长期画像的LT车辆各OD次数统计数据的保存地址
        'LT_OD_num': './4.poratry_data/last_week_data/LT_OD_num.csv',
        # one day长期画像的LT车辆各OD次数统计数据的保存地址
        'back_LT_OD_num': './4.poratry_data/back_LT_OD_num/',
        # lastly day长期画像的LT车辆各OD次数统计数据的保存地址
        'last_LT_OD_num': './4.poratry_data/last_LT_OD_num/',

        # many days长期画像收费部分特征1的保存地址
        'vehicle_portray_combine': './4.poratry_data/vehicle_portray_combine.csv',
        # one day长期画像收费部分特征1的保存地址
        'back_vehicle_portray_combine': './4.poratry_data/back_vehicle_portray_combine/',
        # lastly day长期画像收费部分特征1的保存地址
        'last_vehicle_portray_combine': './4.poratry_data/last_vehicle_portray_combine/',

        # many days长期画像收费部分特征2的保存地址
        'vehicle_portray_result': './4.poratry_data/vehicle_portray_result.csv',
        # one day长期画像收费部分特征2的保存地址
        'back_vehicle_portray_result': './4.poratry_data/back_vehicle_portray_result/',
        # lastly day长期画像收费部分特征2的保存地址
        'last_vehicle_portray_result': './4.poratry_data/last_vehicle_portray_result/',

        # many days长期画像绿通特征的保存地址
        'vehicle_portray_LT': './4.poratry_data/vehicle_portray_LT.csv',
        # one day长期画像绿通特征的保存地址
        'back_vehicle_portray_LT': './4.poratry_data/back_vehicle_portray_LT/',
        # lastly day长期画像绿通特征的保存地址
        'last_vehicle_portray_LT': './4.poratry_data/last_vehicle_portray_LT/',

        # many days特殊处理后OD次数数据的存储地址
        'treated_OD_num_data': './4.poratry_data/last_week_data/treated_OD_num_data.csv',
        # 一天的LT长期middle数据的存储地址
        'LT_middle_one_path': './2.oneDay/back_data/LT_middle/',
        # 多日的LT长期middle数据的存储地址
        'LT_middle_many_path': '../LT/feature_data/',

        # 最近一周的车辆各OD次数统计合并数据的保存地址
        'whole_OD_num_week': './4.poratry_data/last_week_data/whole_OD_num_',
        # 最近一周的车辆上下高速时段次数合并数据的保存地址
        'whole_time_num_week': './4.poratry_data/last_week_data/whole_time_num_',
        # 最近一周的LT车辆各OD次数合并数据的保存地址
        'LT_OD_num_week': './4.poratry_data/last_week_data/LT_OD_num_',
        # 最近一周的长期画像收费部分特征1合并数据的保存地址
        'vehicle_portray_combine_week': './4.poratry_data/last_week_data/vehicle_portray_combine_',
        # 最近一周的长期画像收费部分特征2合并数据的保存地址
        'vehicle_portray_result_week': './4.poratry_data/last_week_data/vehicle_portray_result_',
        # 最近一周的长期画像绿通特征合并数据的保存地址
        'vehicle_portray_LT_week': './4.poratry_data/last_week_data/vehicle_portray_LT_',
        # 最近一周的LT长期middle数据合并数据的保存地址
        'LT_middle_week': './4.poratry_data/last_week_data/LT_middle_',

        # history的长期画像收费部分特征1合并数据的保存地址
        'vehicle_portray_combine_history': './4.poratry_data/history_data/vehicle_portray_combine.csv',
        # history的长期画像收费部分特征2合并数据的保存地址
        'vehicle_portray_result_history': './4.poratry_data/history_data/vehicle_portray_result.csv',
        # history的长期画像收费all特征合并数据的保存地址
        'vehicle_portray_total_history': './4.poratry_data/history_data/vehicle_portray_total.csv',
        # history的LT长期画像收费all特征合并数据的保存地址
        'vehicle_portray_total_LT': './4.poratry_data/history_data/vehicle_portray_total_LT.csv',
        # history的not LT长期画像收费all特征合并数据的保存地址
        'vehicle_portray_total_gantry': './4.poratry_data/history_data/vehicle_portray_total_gantry.csv',
        # history的LT长期画像数据的保存地址
        'vehicle_portray_LT_history': './4.poratry_data/history_data/vehicle_portray_LT.csv',
        # history的车辆各OD次数统计合并数据的保存地址
        'whole_OD_num_history': './4.poratry_data/history_data/whole_OD_num.csv',
        # history的车辆上下高速时段次数合并数据的保存地址
        'whole_time_num_history': './4.poratry_data/history_data/whole_time_num.csv',
        # history的LT车辆各OD次数合并数据的保存地址
        'LT_OD_num_history': './4.poratry_data/history_data/LT_OD_num.csv',
        # history的LT长期middle数据合并数据的保存地址
        'LT_middle_history': './4.poratry_data/history_data/LT_middle.csv',
        # history特殊处理后OD次数数据的存储地址
        'LT_OD_num_treated_history': './4.poratry_data/history_data/treated_OD_num_data.csv',
        # history特殊处理后OD次数数据的存储地址
        'repeat_vehicle_path': './4.statistic_data/repeat_vehicle_part_data_day/',

        # 单次画像数据存储位置
        # 单日正常缺失数据的存储位置
        'part_data_one_path': './3.part_of_data/last_part_data/last_day_part_data.csv',
        # 多日正常缺失数据的存储位置
        'part_data_many_path': './3.part_of_data/last_part_data/part_of_data.csv',

        # 单日异常缺失数据的存储位置
        'loss_data_one_path': './3.loss_data/last_loss_data/',
        # 单日异常缺失数据的备份存储位置
        'loss_data_one_back_path': './3.loss_data/back_loss_data/',
        # 多日异常缺失数据的存储位置
        'loss_data_many_path': './3.loss_data/partTime_loss_data/',
        # 近一周异常缺失合并的数据的存储位置
        'loss_data_week_path': './3.loss_data/last_week_loss_data/',

        # 最小费额数据的地址位置
        'short_data_path': '../Data_Origin/short.csv',

        # 单日合并后跨省数据的存储位置
        'data_province_one_path': './3.short_data/last_short_pro_data/',
        # 合并后跨省数据的备份存储位置
        'data_province_back_path': './3.short_data/back_short_pro_data/',
        # 单日合并后省内数据的存储位置
        'data_whole_one_path': './3.short_data/last_short_whole_data/',
        # 单日合并后省内数据的备份存储位置
        'data_whole_back_path': './3.short_data/back_short_whole_data/',
        # 多日合并后跨省数据的存储位置
        'data_province_many_path': './3.short_data/total_pro_data/',
        # 多日合并后省内数据的存储位置
        'data_whole_many_path': './3.short_data/total_whole_data/',
        # 近一周省内数据合并的数据的存储位置
        'data_whole_week_path': './3.short_data/last_week_whole_data/',
        # 近一周跨省数据合并的数据的存储位置
        'data_pro_week_path': './3.short_data/last_week_pro_data/',

        # 单日稽查特征数据的存储位置
        'feature_data_one_path': './3.whole_data_and_feature/last_feature_data/',
        # 单日稽查特征数据的备份存储位置
        'feature_data_one_back_path': './3.whole_data_and_feature/back_feature_data/',
        # 多日稽查特征数据的存储位置
        'feature_data_many_path': './3.whole_data_and_feature/partTime_feature_data/',

        # 单日稽查结果数据的存储位置
        'result_data_one_path': './3.check_result/last_result_data/',
        # 单日稽查结果数据的备份存储位置
        'result_data_one_back_path': './3.check_result/back_result_data/',
        # 多日稽查结果数据的存储位置
        'result_data_many_path': './3.check_result/partTime_result_data/',
        # 近一周跨省数据合并的数据的存储位置
        'result_data_week_path': './3.check_result/last_week_result_data/',
        # 多日LT稽查结果数据的存储位置
        'LT_result_data_many_path': './3.check_result/partTime_result_data_LT/',
        # 多日gantry稽查结果数据的存储位置
        'gantry_result_data_many_path': './3.check_result/partTime_result_data_gantry/',
        # 多日gantry稽查treat数据的存储位置
        'gantry_result_data_treat_path': './3.check_result/partTime_result_data_gantry_treat/',

        # 单日稽查结果combine LT数据的存储位置
        'result_LT_data_one_path': './3.check_result/result_combine_data/',
        # 单日稽查结果combine no LT数据的存储位置
        'result_vehicle_data_one_path': './3.check_result/result_vehicle_data/',

        # 有套牌风险的车辆信息的存储位置
        'repeat_vehicle_part_data_path': './4.statistic_data/repeat_vehicle_part_data_day/',
        'repeat_vehicle_whole_data_path': './4.statistic_data/repeat_vehicle_whole_data_day/',

        # 交控各异常车辆数据的存储位置
        'JK_abnormal_data': './3.check_result/abnormal_data/',

        # 神经网络风险度预测模型各参数的存储地址
        'network_save_path': './2.Network_Parameter/',

        # 神经网络风险度预测模型中，进行各参数归一化的最大和最小值对应表
        'network_max': './2.Network_Parameter/max.pkl',
        'network_min': './2.Network_Parameter/min.pkl',

        # 进行信用评价模型所需的各异常特征数据表保存位置
        'credit_data_path': './3.vehicle_credit_data/basic_data/',

        # 进行时间衰减之后的所有车辆各长期画像的次数
        'time_treat_portray_path': './3.vehicle_credit_data/portray_data/poraty_of_vehicle.csv',

        # 每一个异常特征的失信积分origin表
        'weight_of_features_origin': './3.vehicle_credit_data/portray_data/weight_of_features_origin.csv',

        # 每一个异常特征的对应失信积分表
        'weight_of_features_path': './3.vehicle_credit_data/portray_data/weight_of_features_new.csv',

        # 所有车辆的长期失信积分数据
        'total_weight_of_features_path': './3.vehicle_credit_data/portray_data/poraty_of_vehicle_weight.csv',

        # 黑名单车辆的长期画像数据
        'abnormal_vehicle_list_path': './3.vehicle_credit_data/portray_data/abnormal_vehicle_list.csv',

        # 白名单车辆的长期画像数据
        'normal_vehicle_list_path': './3.vehicle_credit_data/portray_data/normal_vehicle_list.csv',

        # 各统计和针对结果数据进行处理的数据结果的保存位置管理
        # statistic data的存储位置
        'statistic_data_path': './4.statistic_data/statistic_total.csv',

        # 某列包含某些值的数据存储地址
        'have_some_value_path': './4.statistic_data/have_value_data.csv',

        # case data path
        'case_data_path': './4.statistic_data/case_data/',
        # case data path
        'case_data_vehicle_path': './4.statistic_data/case_data/abnormal_list_',

        # goal sample data for neuralNetwork
        'neuralNetwork_goal_sample_path': './3.neuralNetwork_sample/goal_sample/',
        # normal sample data for neuralNetwork
        'neuralNetwork_normal_sample_path': './3.neuralNetwork_sample/normal_sample/',

        # 绿通数据库获取的SQL语句（缺少时间条件）
        'LT_features': ' SELECT t1.`VEHICLE_CLASS`,t1.`APPOINTMENT_START_TRANSPORT_TIME`,'
                       't1.`APPOINTMENT_END_TRANSPORT_TIME`,t1.`APPOINTMENT_START_TOLLGATE_ID`,'
                       't1.`APPOINTMENT_END_TOLLGATE_ID`,t1.`APPOINTMENT_USER_ID`,t1.`VEHICLE_ID`,t1.`CHECK_TIME`,'
                       't1.`VEHICLE_TYPE`,t1.`CRATE_TYPE`,t1.`OWNER`,t1.`EXIT_LANE`,t1.`GROUP_ID`,t1.`EN_WEIGHT`,'
                       't1.`EX_WEIGHT`,t1.`WEIGHT`,t1.`ENTRANCE_TOLLGATE_ID`,t1.`EXIT_TOLLGATE_ID`,t1.`INSPECTOR_ID`,'
                       't1.`REVIEWER_ID`,t1.`PASS_ID`,t1.`CHECK_RESULT`,t1.`MEDIA_TYPE`,t1.`FEE`,t1.`REASON`,'
                       't1.`MEMO`,t1.`EXEMPTION`,t1.`UPDATE_BY`,t1.`UPDATE_TIME`,t1.`IS_VALID`,t1.`STATUS`,'
                       't2.`TYPE_ID` FROM `transport` t1,`transport_check_loaded_freight` t2 WHERE '
                       't1.`ID`=t2.`TRANSPORT_ID` AND t1.`CHECK_TIME`>=',
        # 门架收费数据
        'charge_gantry_features': 'SELECT `BIZID`, `PASSID`, `VEHICLEPLATE`, `IDENTIFYVEHICLEID`, `ENTRYSTATION`, '
                                  '`EXITSTATION`, `FEE`, `GANTRYID`, `TRANSTIME`, `ENTIME`, `MEDIATYPE`, '
                                  '`VEHICLETYPE`, `VEHICLECLASS`, `ENTOLLSTATIONNAME`, `ENWEIGHT`, `VEHICLETYPE`, '
                                  '`VEHICLECLASS`, `ENTOLLSTATIONHEX`, `TRADERESULT`, `TOLLINTERVALID` FROM '
                                  '`transport` WHERE `TRANSTIME`>=',
        # 省界门架收费数据
        'charge_gantry_features_pro': 'SELECT `BIZID`, `PASSID`, `VEHICLEPLATE`, `IDENTIFYVEHICLEID`, `ENTRYSTATION`, '
                                      '`EXITSTATION`, `FEE`, `GANTRYID`, `TRANSTIME`, `ENTIME`, `MEDIATYPE`, '
                                      '`VEHICLETYPE`, `VEHICLECLASS`, `ENTOLLSTATIONNAME`, `ENWEIGHT`, `VEHICLETYPE`, '
                                      '`VEHICLECLASS`, `ENTOLLSTATIONHEX`, `TRADERESULT`, `TOLLINTERVALID` FROM '
                                      '`transport` WHERE `TRANSTIME`>=',
        # 入口收费数据
        'charge_in_features': 'SELECT `ENDEALID`, `PASSID`, `ENVEHICLEID`, `ENIDENTIFYVEHICLEID`, `ENTRYSTATION`, '
                              '`FEE`,`ENTIME`, `MEDIATYPE`, `ENVEHICLETYPE`, `GRANTRYTRADEID`, `ENVEHICLECLASS`, '
                              '`ENTOLLSTATIONNAME`,`ENSTATIONHEX`, `ENWEIGHT`, `ENDEALID`, `VEHICLESIGNID`, '
                              '`PICBATCH`, `PICNO`,`VEHICLETAILSIGNID`, `TAILPICBATCH`, `TAILPICNO`, `RECORDTYPE`, '
                              '`PAYTYPE`, `PAYKIND`, `DEALSTATUS` FROM `transport` WHERE `TRANSTIME`>=',
        # 出口收费数据
        'charge_out_features': 'SELECT `EXDEALID`, `PASSID`, `ENVEHICLEID`, `ENIDENTIFYVEHICLEID`, `ENTRYSTATION`, '
                               '`EXVEHICLEID`,`EXIDENTIFYVEHICLEID`, `EXITSTATION`, `TOTALTOLL`, `GRANTRYTRADEID`, '
                               '`EXTIME`, `ENTIME`,`MEDIATYPE`, `ENVEHICLETYPE`, `ENVEHICLECLASS`, `EXVEHICLETYPE`, '
                               '`EXVEHICLECLASS`,`ENTOLLSTATIONNAME`, `OBUVEHICLETYPE`, `ENAXLECOUNT`, `EXAXLECOUNT`, '
                               '`OBUSN`, `ETCCARDID`,`EXITFEETYPE`, `ENSTATIONHEX`, `OBUVEHICLEID`, `CARDVEHICLEID`,'
                               '`ENWEIGHT`, `EXWEIGHT`, `ENDEALID`, `EXDEALID`, `VEHICLESIGNID`, `PICBATCH`, `PICNO`,'
                               '`VEHICLETAILSIGNID`, `TAILPICBATCH`, `TAILPICNO`, `RECORDTYPE`, `PAYTYPE`, `PAYKIND`, '
                               '`DEALSTATUS` FROM `transport` WHERE `TRANSTIME`>=',
        # 牌识收费数据
        'charge_iden_features': 'SELECT * FROM `transport` WHERE `TRANSTIME`>=',

        # 数据库各表的名称、字段、类型的管理
        #
        # 绿通whole result data
        'totaldata_LT_table_name': 'vehicleCheckTotal_data_with_LT_10_100',
        'totaldata_LT_table_features': ["PASSID", "enVehiclePlateTotal", "enIdenVehiclePlate", "enVehiclePlate",
                                        "enPlateColor",
                                        "entryStationID", "entryStationHEX", "entryTime", "enMediaType",
                                        "enVehicleType",
                                        "enVehicleClass", "entryWeight", "enAxleCount", "exVehiclePlateTotal",
                                        "exIdenVehiclePlate",
                                        "exVehiclePlate", "exPlateColor", "exitStationID", "exitTime", "exMediaType",
                                        "exVehicleType",
                                        "exVehicleClass", "exitWeight", "exAxleCount", "obuVehicleType", "obuSn",
                                        "etcCardId",
                                        "ExitFeeType", "exOBUVehiclePlate", "exCPUVehiclePlate", "payFee",
                                        "middle_type", "gantryPath",
                                        "intervalPath", "gantryNum", "gantryTimePath", "gantryTypePath",
                                        "gantryTotalFee",
                                        "gantryFeePath", "gantryTotalLength", "firstGantryTime", "endGantryTime",
                                        "ifProvince", "endsGantryType",
                                        "ifHaveCard", "vehiclePlateTotal", "dataType", "proInGantryId",
                                        "proInStationId",
                                        "proInStationName", "inProTime", "proOutGantryId", "proOutStationId",
                                        "proOutStationName",
                                        "outProTime",

                                        "shortPathFee", "shortPathLength", "shortPath", "shortPathNum",
                                        "ifVehiclePlateMatch",
                                        "ifVehiclePlateSame", "ifPlateColorSame", "ifOBUPlateSame", "ifETCPlateSame",
                                        "ifVehicleTypeSame",
                                        "ifOBUTypeLarger", "ifUseShortFee", "ifVeTypeLargerAxle", "gantryNumRate",
                                        "ifGantryPathWhole",
                                        "gantryPathIntegrity", "gantryPathMatch", "ifGrantryFeeSame", "ifShortFeeSame",
                                        "maxOutRangeTime",
                                        "ifTimeOutRange", "maxOutRangeGantry", "outRangeTimePath", "outRangeGantryPath",
                                        "outRangeSpeedPath", "totalTime", "ifShortTimeAbnormal", "shortOutRangeTime",
                                        "pathType",

                                        "GantryPathAbnormal", "ifVehiclePlateMatchCode", "ifVehiclePlateSameCode",
                                        "ifPlateColorSameCode",
                                        "ifOBUPlateSameCode", "ifETCPlateSameCode", "ifVehicleTypeSameCode",
                                        "ifExAxleLargerCode",
                                        "ifOBUTypeLargerCode", "ifPassTimeLarger3DaysCode",
                                        "ifVehicleTypeSameCode_filter",
                                        "ifPassTimeAbnormalCode", "ifFeeMatchCode", "ifPathTypeAbnormalCode",
                                        "combineCode",
                                        "abnormalType", "abnormalCore",

                                        "createType", "checkTime", "groupID", "inspectorID", "reviewerID",
                                        "checkResult", "reason",
                                        "entranceTollgateName", "entranceLocation", "entranceCity", "entranceProvince",
                                        "exitTollgateName", "exitLocation", "exitCity", "exitProvince", "kindType",
                                        "entranceTime",
                                        "entranceProbability", "exitProbability", "kindWithVehicleProbability",
                                        "twoLocationRelevancy",
                                        "ifWeightNormal", "ifQuickInOut", "inStationIdPub", "outStationIdPub"],
        'totaldata_LT_table_type': ['varchar', 'varchar', 'varchar', 'varchar', 'tinyint', 'varchar', 'varchar',
                                    'datetime', 'int',
                                    'int', 'int', 'int', 'int', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar',
                                    'datetime', 'int', 'int', 'int', 'int', 'int', 'int', 'varchar', 'varchar', 'int',
                                    'varchar', 'varchar', 'int', 'varchar', 'varchar', 'varchar', 'int', 'varchar',
                                    'varchar',
                                    'int', 'varchar', 'int', 'datetime', 'datetime', 'tinyint', 'varchar', 'tinyint',
                                    'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'datetime', 'varchar',
                                    'varchar', 'varchar', 'datetime',

                                    'int', 'int', 'varchar', 'int', 'tinyint', 'tinyint', 'tinyint', 'tinyint',
                                    'tinyint', 'tinyint', 'tinyint', 'tinyint', 'tinyint', 'float', 'tinyint', 'int',
                                    'int',
                                    'tinyint', 'tinyint', 'int', 'tinyint', 'varchar', 'varchar', 'varchar', 'varchar',
                                    'int',
                                    'tinyint', 'int', 'varchar',

                                    'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar',
                                    'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar',
                                    'varchar',
                                    'varchar', 'float',

                                    "varchar", "datetime", "int", "varchar", "varchar", "tinyint", "int", "varchar",
                                    "varchar",
                                    "varchar", "varchar", "varchar", "varchar", "varchar", "varchar", "varchar",
                                    "datetime",
                                    "float", "float", "float", "float", "tinyint", "tinyint", "varchar", "varchar"],

        # 绿通OD次数原始数据
        'LT_OD_table_name': 'LT_OD_number',
        'LT_OD_table_features': ["id", "vehiclePlate", "stationID_1", "stationID_2", "stationName_1", "stationName_2",
                                 "locationName_1", "locationName_2", "cityName_1", "cityName_2", "provinceName_1",
                                 "provinceName_2", "number"],
        'LT_OD_table_type': ['int', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar',
                             'varchar',
                             'varchar', 'varchar', 'varchar', 'int'],

        # 绿通OD次数处理后数据
        'LT_OD_treated_table_name': 'LT_OD_number_treated',
        'LT_OD_treated_table_features': ["id", "vehiclePlate", "cityName_1", "cityName_2", "number"],
        'LT_OD_treated_table_type': ['int', 'varchar', 'varchar', 'varchar', 'int'],

        # 全路网OD次数原始数据
        'gantry_OD_table_name': 'gantry_OD_number',
        'gantry_OD_table_features': ["id", "vehiclePlate", "stationID_1", "stationID_2", "cityName_1", "cityName_2",
                                     "number"],
        'gantry_OD_table_type': ['int', 'varchar', 'varchar', 'varchar', 'varchar', 'varchar', 'int'],

        # 全路网上下高速时段原始数据
        'inout_time_table_name': 'inout_time_number',
        'inout_time_table_features': ["vehiclePlate", "zeroClock", "oneClock", "twoClock", "threeClock",
                                      "fourClock", "fiveClock", "sixClock", "sevenClock", "eightClock", "nineClock",
                                      "tenClock", "elevenClock", "twelveClock", "thirteenClock", "fourteenClock",
                                      "fifteenClock", "sixteenClock", "seventeenClock", "eighteenClock",
                                      "nineteenClock", "twentyClock", "twentyOneClock", "twentyTwoClock",
                                      "twentyThreeClock"],
        'inout_time_table_type': ['varchar', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int',
                                  'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int',
                                  'int', 'int'],

        # LT长期画像原始数据
        'long_portray_LT_table_name': 'portrayFeature_gantry_data',
        'long_portray_LT_table_features': ["vehiclePlateTotal", "passNumTotal", "passNumInProvince",
                                           "passNumOutProvince", "outLossNumber",
                                           "inLossNumber", "lossNumber", "freePassNum", "notFreePassNum",
                                           "frequencyPath",
                                           "frequencyPathNum", "ODNum", "ODAVGNum", "recentlyPath", "LTPassNum",
                                           "weightChangeNum", "shortFeeNum", "oftenInTime", "oftenOutTime",
                                           "ifObuSnChange", "ifEtcSnChange", "gantryLossNum", "vehiclePlateDiffNum",
                                           "OBUPlateDiffNum", "ETCPlateDiffNum", "noGantryPathNum", "ETCPassNum",
                                           "MTCPassNum", "vehicleTypeDiffNum", "stayLongNum", "passTimeLargerNum",
                                           "UPathNum", "JPathNum", "repeatPathNum", "passTimeAbnormalNum",
                                           "payFeeAbnormalNum", "exAxleLargerNum", "ifVehicleClassChange",
                                           "ifVehicleTypeChange", "LTProvinceOutNum", "LTProvinceRate",
                                           "LTPassAbnormalNum",
                                           "LTCheatNum", "LTFrequencyGoods", "LTFrequencyGoodsNum", "LTFrequencyCity",
                                           "LTFrequencyCityNum", "LTFrequencyCreate"],
        'long_portray_LT_table_type': ['varchar', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int',
                                       'varchar',
                                       'int', 'int', 'float', 'varchar', 'int', 'int', 'varchar', 'varchar', 'tinyint',
                                       'tinyint', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int',
                                       'int',
                                       'int', 'int', 'int', 'int', 'int', 'tinyint', 'tinyint', 'int', 'int', 'int',
                                       'int',
                                       'varchar', 'varchar', 'varchar', 'varchar', 'varchar'],
        # gantry长期画像原始数据
        'long_portray_gantry_table_name': 'portrayFeature_gantry_data',
        'long_portray_gantry_table_feature': ["vehiclePlateTotal", "passNumTotal", "passNumInProvince",
                                              "passNumOutProvince", "outLossNumber", "inLossNumber", "lossNumber",
                                              "freePassNum", "notFreePassNum", "frequencyPath",
                                              "frequencyPathNum", "ODNum", "ODAVGNum", "recentlyPath", "LTPassNum",
                                              "weightChangeNum", "shortFeeNum", "oftenInTime", "oftenOutTime",
                                              "ifObuSnChange", "ifEtcSnChange", "gantryLossNum", "vehiclePlateDiffNum",
                                              "OBUPlateDiffNum", "ETCPlateDiffNum", "noGantryPathNum", "ETCPassNum",
                                              "MTCPassNum", "vehicleTypeDiffNum", "stayLongNum", "passTimeLargerNum",
                                              "UPathNum", "JPathNum", "repeatPathNum", "passTimeAbnormalNum",
                                              "payFeeAbnormalNum", "exAxleLargerNum", "ifVehicleClassChange",
                                              "ifVehicleTypeChange"],
        'long_portray_gantry_table_type': ['varchar', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int',
                                           'varchar', 'int', 'int', 'float', 'varchar', 'int', 'int', 'int', 'varchar',
                                           'varchar', 'tinyint',
                                           'tinyint', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int',
                                           'int', 'int',
                                           'int', 'int', 'int', 'int', 'int', 'tinyint', 'tinyint'],

        # 西宝各门架流量和速度数据表的相关信息
        'flow_base_name': 'interval_flow_base',
        'flow_base_features': ["INTERVAL_ID", "TIME_POINT", "TIME_POINT_ORDER", "INSERT_TIME", "UPDATE_TIME", "STATUS",
                               "FLOW_1", "FLOW_2", "FLOW_3", "FLOW_4", "FLOW_11", "FLOW_12", "FLOW_13", "FLOW_14",
                               "FLOW_15", "FLOW_16", "SPEED_1", "SPEED_2", "SPEED_3", "SPEED_4", "SPEED_11", "SPEED_12",
                               "SPEED_13", "SPEED_14", "SPEED_15", "SPEED_16"],
        'flow_base_features_type': ['varchar', 'datetime', 'int', 'datetime', 'datetime', 'int', 'int', 'int', 'int',
                                    'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int',
                                    'int', 'int', 'int', 'int', 'int'],

        # 各收费站流量数据表的相关信息
        'station_flow_base_name': 'station_flow',
        'station_flow_base_features': ["STATION_ID", "TIME_POINT", "TIME_POINT_ORDER", "DIRECT", "CLOSEST_GANTRY_ID",
                                       "STATUS", "FLOW_1", "FLOW_2", "FLOW_3", "FLOW_4", "FLOW_11", "FLOW_12", "FLOW_13",
                                       "FLOW_14", "FLOW_15", "FLOW_16"],
        'station_flow_base_features_type': ['varchar', 'datetime', 'int', 'int', 'varchar', 'int', 'int', 'int', 'int',
                                            'int', 'int', 'int', 'int', 'int', 'int', 'int'],

        # 西宝各门架输入输出数据表的相关信息
        'interval_flow_name': 'interval_flow',
        'interval_flow_features': ["INTERVAL_ID", "TIME_POINT", "TIME_POINT_ORDER", "IN_1", "IN_2", "IN_3", "IN_4",
                                   "IN_11", "IN_12", "IN_13", "IN_14", "IN_15", "IN_16", "IN_TOTAL", "OUT_1", "OUT_2",
                                   "OUT_3", "OUT_4", "OUT_11", "OUT_12", "OUT_13", "OUT_14", "OUT_15", "OUT_16",
                                   "OUT_TOTAL", "HAVE_1", "HAVE_2", "HAVE_3", "HAVE_4", "HAVE_11", "HAVE_12", "HAVE_13",
                                   "HAVE_14", "HAVE_15", "HAVE_16", "HAVE_TOTAL"],
        'interval_flow_features_type': ['varchar', 'datetime', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int',
                                        'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int',
                                        'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int',
                                        'int', 'int', 'int', 'int'],

        # 西宝各路段拥堵预警数据表的相关信息
        'interval_congestion_name': 'interval_congestion',
        'interval_congestion_features': ["ROAD_ID", "INTERVAL_ID", "TIME_POINT", "START_STAKE_NUM", "END_STAKE_NUM",
                                         "CONGESTION_LEVEL", 'STATUS_OF_CONGESTION', 'LOADING_NUM', 'CONGESTION_LENGTH',
                                         'CONGESTION_DURATION'],
        'interval_congestion_features_type': ['varchar', 'varchar', 'datetime', 'int', 'int', 'tinyint', 'tinyint',
                                              'int', 'int', 'int'],

        # 西宝路段拥堵长度数据表的相关信息
        'interval_congestion_detail_name': 'interval_congestion_detail',
        'interval_congestion_detail_features': ["INTERVAL_ID", "ROAD_ID", "TIME_POINT", "EVENT_STAKE_NUM",
                                                "COMPUTE_TIME", "CALCULATE_DURATION", "CONGESTION_RESULT"],
        'interval_congestion_detail_features_type': ['varchar', 'varchar', 'datetime', 'int', 'datetime', 'int', 'int'],

        # 各门架输入输出数据upload time表的相关信息
        'interval_flow_upload_name': 'interval_flow_upload_time',
        'interval_flow_upload_features': ["UPLOAD_TIME"],
        'interval_flow_upload_features_type': ['varchar'],

        # 各路段拥堵预警数据upload time表的相关信息
        'interval_congestion_upload_name': 'interval_congestion_upload_time',
        'interval_congestion_upload_features': ["UPLOAD_TIME"],
        'interval_congestion_upload_features_type': ['varchar'],

    }
    if type(keys) == list:
        para_list = []
        for key in keys:
            para_list.append(parameter_disc[key])
        return para_list
    else:
        return parameter_disc[keys]
