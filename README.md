# DIRestPlus 
[![Build Status](https://travis-ci.org/DataIntegrationAlliance/DIRestPlus.svg?branch=master)](https://travis-ci.org/DataIntegrationAlliance/DIRestPlus)
[![GitHub issues](https://img.shields.io/github/issues/DataIntegrationAlliance/DIRestPlus.svg)](https://github.com/DataIntegrationAlliance/DIRestPlus/issues)
[![GitHub forks](https://img.shields.io/github/forks/DataIntegrationAlliance/DIRestPlus.svg)](https://github.com/DataIntegrationAlliance/DIRestPlus/network)
[![GitHub stars](https://img.shields.io/github/stars/DataIntegrationAlliance/DIRestPlus.svg)](https://github.com/DataIntegrationAlliance/DIRestPlus/stargazers)
[![GitHub license](https://img.shields.io/github/license/DataIntegrationAlliance/DIRestPlus.svg)](https://github.com/DataIntegrationAlliance/DIRestPlus/blob/master/LICENSE)
[![HitCount](http://hits.dwyl.io/DataIntegrationAlliance/https://github.com/DataIntegrationAlliance/DIRestPlus.svg)](http://hits.dwyl.io/DataIntegrationAlliance/https://github.com/DataIntegrationAlliance/DIRestPlus)
[![Pypi](https://img.shields.io/badge/pypi-wheel-blue.svg)](https://pypi.org/project/DIRestPlus/)
[![Twitter](https://img.shields.io/twitter/url/https/github.com/DataIntegrationAlliance/DIRestPlus.svg?style=social)](https://twitter.com/intent/tweet?text=Wow:&url=https%3A%2F%2Fgithub.com%2FDataIntegrationAlliance%2FDIRestPlus)

Data Integration RESTPlus，通过Flask-RESTPlus 构建接口框架，将Wind、iFinD、Choice等进行统一封装

## 启动（两步）
1. 编写一份启动脚本
run.py 文件
```python
from direstplus import start_service
from direstplus.config import config
# 配置 ifind 账号、密码
config.THS_LOGIN_USER_NAME = '***'
config.THS_LOGIN_PASSWORD = '***'
# 
start_service()
```
2. 启动脚本
```commandline
python run.py
```
如果是Linux系统，使用python3
```bash
python3 run.py
```

## 外部环境依赖及安装配置

### ifind 环境配置
```commandline
python "d:\IDE\iFinD\bin\x64\installiFinDPy.py" "D:\IDE\iFinD\bin"
```

#### 测试接口文件安装是否成功
```python
from iFinDPy import *
# 返回 D:\WSPych\RestIFindPy\venv\lib\site-packages\iFinDPy.pth
# 说明安装成功

# 登陆
thsLogin = THS_iFinDLogin("***","***")  # 0

thsDataDataPool  = THS_DataPool('block','2016-11-27;001005260','date:Y,security_name:Y,thscode:Y')
```
-----

### wind 环境配置
```commandline
python "d:\IDE\Wind\Wind.NET.Client\WindNET\bin\installWindPy.py" "d:\IDE\Wind\Wind.NET.Client\WindNET"
```

#### 测试接口文件安装是否成功
```python
from WindPy import w
w.start()
```
输出内容：
> Welcome to use Wind Quant API for Python (WindPy)!
COPYRIGHT (C) 2017 WIND INFORMATION CO., LTD. ALL RIGHTS RESERVED.
IN NO CIRCUMSTANCE SHALL WIND BE RESPONSIBLE FOR ANY DAMAGES OR LOSSES CAUSED BY USING WIND QUANT API FOR Python.
.ErrorCode=0
.Data=[OK!]
