# -*- coding: UTF-8 -*-
import requests
import json

'''
    函数库简介：管理所有接口功能函数
    建立时间：2022/11/11
'''


class interface_manager:

    def __init__(self, target, data):
        """
        初始化接口函数
        :param target: 接口目标特征
        """
        if target == 'ETC_enable':
            self.data = json.dumps(data)
            self.url = "http://192.168.0.204:8560/FreewayPublicServer/services/rest/freewayShuntPush/congestionComplete"
            self.headers = {"content-type": "application/json; charset=utf-8"}

        elif target == 'XB_congestion':
            print(1)

    def request_function(self):
        response = requests.post(url=self.url, headers=self.headers, data=self.data)
        return response
