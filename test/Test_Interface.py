# -*- coding: UTF-8 -*-
# 进行接口函数库的测试

import Interface
import pytest

'''
    创建时间：2022/11/11
    修改时间：2022/11/11
    内容：ETC赋能接口函数的测试
'''


def test_interface_ETC():
    interface = Interface.interface_manager('ETC_enable')
    result = interface.request_function()
    print('\t')
    print('数据格式:', type(interface.json))
    print('地址:', interface.url)
    print('header内容:', interface.headers)
    print('返回结果:', result)