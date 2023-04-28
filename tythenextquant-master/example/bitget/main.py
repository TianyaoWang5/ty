# -*- coding:utf-8 -*-

"""
OKx 模块使用演示

为了在订单薄买盘提前埋伏订单，在 `BTC/USDT` 订单薄盘口距离10美金的位置挂买单，数量量为1。
随着订单薄盘口价格不断变化，需要将价格已经偏离的订单取消，再重新挂单，使订单始终保持距离盘口价差为 `10 ± 1` 美金。
这里设置了缓冲价差为 `1` 美金，即只要盘口价格变化在 `± 1` 内，都不必撤单之后重新挂单，这样设置的目的是尽量减少挂撤单的次数，
因为交易所开放的交易接口有调用频率的限制，如果调用太过频繁超过了限制可能会报错。
"""

import sys

from quant import const
from quant.utils import tools
from quant.utils import logger
from quant.config import config
from quant.order import ORDER_ACTION_BUY, ORDER_STATUS_FAILED, ORDER_STATUS_CANCELED, ORDER_STATUS_FILLED
from quant.platform import okx
from quant.platform import binance
from quant.platform import bitget
from quant.utils import sqlite3db
from quant.tasks import LoopRunTask, SingleTask
import time
from quant.utils.decorator import async_method_locker

class MyStrategy:
    def __init__(self):
        """ 初始化
        """
        self.strategy = config.strategy
        self.platform = const.OKX
        self.account = config.accounts[0]["account"]
        self.access_key = config.accounts[0]["access_key"]
        self.secret_key = config.accounts[0]["secret_key"]
        self.host = "https://api.binance.com"
        self.ba_rest_api = binance.BinanceRestAPI(self.host,self.access_key,self.secret_key)
        self.account = config.accounts[1]["account"]
        self.access_key = config.accounts[1]["access_key"]
        self.secret_key = config.accounts[1]["secret_key"]
        self.passphrase = config.accounts[1]["passphrase"]
        self.host = "https://api.bitget.com"
        self.bitget_rest_api = bitget.BitgetRestAPI(self.host,self.access_key,self.secret_key,self.passphrase)            
        self.symbol = config.symbol
        self.TY = sqlite3db.Sqlitedb(Logtocmd=True)  # 转为实盘后，需要把参数改为False
        self.proxy = config.proxy  # 转为实盘后，需要把这个参数注释掉
        '''循环同步执行Main_Loop,间隔最小单位0.1秒'''
        self.period_times = 0   #执行第一次循环时的时间戳
        SingleTask.run(self.anyncgo)

    @async_method_locker("anyncgo", wait=True)
    async def anyncgo(self, *args, **kwargs):
        while True:
            time_sign = time.time()
            if(time_sign-self.period_times < 10):   # 如0.7秒,则主循环时间间隔为0.701~0.799(保留3位)
                time.sleep(0.1)
                continue
            self.period_times = time.time()
            await self.Main_Loop()
    
    async def Main_Loop(self, *args, **kwargs):
        self.TY.Log("进入主循环")
        param_bitget = {  # 深度信息，40次/2s，400档
            "symbol": self.symbol.replace("_","")+"_SPBL",
        }
        param_bitget_io = {  # 深度信息，40次/2s，400档
            # "instId": self.symbol,
            # "sz": 400,
        }
        # result_bitget, error ,rethead= await self.bitget_rest_api.HttpQuery("GET", "https://api.bitget.com/api/spot/v1/market/fills",param_bitget,rethead=True)
        # self.TY.Log("Bitget1:",result_bitget, error)
        # result_bitget, error ,rethead= self.bitget_rest_api.synHttpQuery("GET", "https://api.bitget.com/api/spot/v1/market/fills",param_bitget,rethead=True)
        # self.TY.Log("Bitget2:",result_bitget)
        # result_bitget, error ,rethead= await self.bitget_rest_api.exchangeIO("GET", "/api/spot/v1/account/assets",param_bitget_io,rethead=True)
        # self.TY.Log("Bitget3:",result_bitget)
        # result_bitget, error ,rethead= self.bitget_rest_api.synexchangeIO("GET", "/api/spot/v1/account/assets",param_bitget_io,rethead=True)
        # self.TY.Log("Bitget4:",result_bitget, error)
        
        param_bitget = {  # 深度信息，40次/2s，400档
            "symbol": self.symbol,
        }
        # result_bitget, error= await self.bitget_rest_api.GetTicker("BTC_USDT", autotry=True,sleep=3000)
        # self.TY.Log("Bitget:",result_bitget, error)
        # result_bitget, error = await self.bitget_rest_api.GetDepth("BTC_USDT", autotry=True, sleep=3000)
        # self.TY.Log("Bitget:",result_bitget, error)
        # result_bitget, error = await self.bitget_rest_api.GetTrades("BTC_USDT", autotry=True, sleep=3000)
        # self.TY.Log("Bitget:",result_bitget, error)
        # result_bitget, error = await self.bitget_rest_api.GetAccount("BTC","USDT", autotry=True, sleep=3000)
        # self.TY.Log("Bitget:",result_bitget, error)
        result_bitget, error = await self.bitget_rest_api.Buy("BTC_USDT", 10000,0.001)
        self.TY.Log("Bitget:",result_bitget, error)
        # time.sleep(10)
        # result_bitget, error = await self.bitget_rest_api.CancelOrder("BTC_USDT")
        # self.TY.Log("Bitget:",result_bitget, error)

def main():
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    else:
        config_file = None

    from quant.quant import quant
    quant.initialize(config_file)
    MyStrategy()
    quant.start()


if __name__ == '__main__':
    main()
