# -*- coding:utf-8 -*-

# 策略实现

from multiprocessing.connection import wait
import sys
import traceback
from random import random
from quant import const
from quant.quant import quant
from quant.utils import tools
from quant.utils import logger
from quant.config import config
from quant.market import Market
from quant.trade import Trade
from quant.const import BINANCE
from quant.order import Order
from quant.market import Orderbook
from quant.order import ORDER_ACTION_BUY, ORDER_STATUS_FAILED, ORDER_STATUS_CANCELED, ORDER_STATUS_FILLED,ORDER_ACTION_SELL
from quant.tasks import LoopRunTask,SingleTask
from quant.platform import binance
import math
from quant.utils.decorator import async_method_locker
import time
import asyncio
import nest_asyncio

import random

nest_asyncio.apply()

class MyStrategy:

    def __init__(self):
        """ 初始化
        """
        
        try:
            self.strategy = config.strategy
            self.platform = config.accounts[0]["platform"]
            self.account = config.accounts[0]["account"]
            self.access_key = config.accounts[0]["access_key"]
            self.secret_key = config.accounts[0]["secret_key"]
            self.host = "https://api.binance.com"
            self.ba_rest_api = binance.BinanceRestAPI(self.host,self.access_key,self.secret_key)
            self._T = None
        
            #自定义参数
            self.basesymbol = config.strategyargs["basesymbol"]

        except Exception as error:
            # 发生错误时回滚
            print(str(error))
            print("\n" + traceback.format_exc())

        self.trade_info = {}
        self.assets = {}
        self.midprice = 0
        self.perlooptime = 0
        self.ticker = {}
        self.bidPrice = 0
        self.askPrice = 0
        self.orderbook = None
        self.accuptime = 0
        self.servertime = 0
        self.loopnum = 0
        self.islog = False
        self.initbalance = 0

        
        print("策略初始化完成")
        
        #SingleTask.run(self.updateexinfo)
        LoopRunTask.register(self.mainloop,3)

        # 订阅行情
        #Market(const.MARKET_TYPE_ORDERBOOK, self.platform, self.symbol, self.mainloop)
    def inittrade(self):
        # 交易模块
        cc = {
            "strategy": self.strategy,
            "platform": self.platform,
            "symbol": self.symbol,
            "account": self.account,
            "access_key": self.access_key,
            "secret_key": self.secret_key,
            "order_update_callback": self.on_event_order_update,
        }
        self.trader = Trade(**cc)

    async def gotasks(self):
        tasks = [asyncio.create_task(self.get_orderbook_update()), asyncio.create_task(self.check_asset_update()),asyncio.create_task(self.get_bookTicker_update()),asyncio.create_task(self.get_server_time_update())]
        done,pending = await asyncio.wait(tasks)

    async def anyncgo(self,*args, **kwargs):
        asyncio.run(self.gotasks())

    async def mainloop(self,*args, **kwargs):
        """ 订单薄更新
        """
        #机器人循环状态参数，1为正常循环，2为进入tyonexit()
        try:
            self.robotloopcmd = config.robotloopcmd

            #自定义参数
            print(config.strategyargs)
            self.basesymbol = config.strategyargs["basesymbol"]
            print(self.basesymbol)
           

            
        except Exception as error:
            # 发生错误时回滚
            print(str(error))
            print("\n" + traceback.format_exc())
    async def coverpos(self):
        #清仓并停止
        while (self.assets["Stocks"]+self.assets["FrozenStocks"])*self.bidPrice>self.trade_info[self.symbol]['minvalues']:
            await self.cancelorders()
            await self.sell(self.assets["Stocks"]+self.assets["FrozenStocks"],round(self.askPrice,self.trade_info[self.symbol]['priceSize']))
            time.sleep(2) 
            await self.check_asset_update()
            await self.get_bookTicker_update()
        return
    
    async def tyonexit(self):
        print("进入扫尾")       
        await self.coverpos()
        quant.stop()

    #计算将要下单的价格
    def GetPrice(self,ptype,depth) :
        amountBidsvalues=0
        amountAsksvalues=0
        if(ptype=="Buy"):
            for i in range(20):
                amountBidsvalues+=float(depth["bids"][i][0])*float(depth["bids"][i][1])
                if (amountBidsvalues>self.floatvaluesbuy):
                    return float(depth["bids"][i][0])+self.trade_info[self.symbol]["tickSize"]
            return float(depth["bids"][0][0])
            
        if(ptype=="Sell"):
            for j in range(20):
                amountAsksvalues+=float(depth["asks"][j][0])*float(depth["asks"][j][1])
                if (amountAsksvalues>self.floatvaluessell):
                    return float(depth["asks"][j][0])-self.trade_info[self.symbol]["tickSize"]
            return float(depth["asks"][0][0])

    async def on_event_order_update(self, order: Order):
        """ 订单状态更新
        """
        logger.info("order update:", order, caller=self)

        # 如果订单失败、订单取消、订单完成交易
        if order.status in [ORDER_STATUS_FAILED, ORDER_STATUS_CANCELED, ORDER_STATUS_FILLED]:
            pass
            #self.order_no = None

    async def updateexinfo (self,*args, **kwargs):
        success, error = await self.ba_rest_api.get_exchange_info()
        if success:
            for sym in success["symbols"]:
                if sym["symbol"] == self.symbol:
                    self.trade_info[self.symbol] = {}                    
                    for j in range(len(sym["filters"])):
                        if (sym["filters"][j]["filterType"]=="LOT_SIZE"):
                            self.trade_info[self.symbol]["minQty"] = float(sym["filters"][j]["minQty"])
                            self.trade_info[self.symbol]['amountSize'] = int(math.log10(1.1 / float(sym["filters"][j]['stepSize'])))
                        
                        if (sym["filters"][j]['filterType']=="PRICE_FILTER"):
                            self.trade_info[self.symbol]['priceSize'] = int(math.log10(1.1 / float(sym["filters"][j]['tickSize'])))        
                            self.trade_info[self.symbol]['tickSize'] = float(sym["filters"][j]['tickSize'])
                        
                        if (sym["filters"][j]['filterType']=="MIN_NOTIONAL"):
                            self.trade_info[self.symbol]['minvalues'] = float(sym["filters"][j]['minNotional'])
                    logger.debug(sym["filters"],caller = self)
                    logger.debug("交易精度",self.trade_info,caller = self)       
                    break     
        else:
            logger.info(error,caller = self)
    
    async def get_orderbook_update (self,*args, **kwargs):
        success, error = await self.ba_rest_api.get_orderbook(self.symbol,limit = 20)
        if success:
            self.orderbook = success  
            logger.debug("get_orderbook_update",self.orderbook,caller = self)

        else:
            logger.info("get_orderbook_update",error,caller = self)

    async def check_asset_update(self, *args, **kwargs):
        """Fetch asset information."""
        result, error = await self.ba_rest_api.get_user_account()
        if error:
            logger.warn("platform:", self.platform, "account:", self.account, "get asset info failed!", caller=self)
            return

        assets = {
                "Info": {},
                "Balance":0,
                "FrozenBalance":0,
                "Stocks":0,
                "FrozenStocks":0,
            }
        self.accuptime = int(result["updateTime"])
        
        for item in result["balances"]:
            name = item.get("asset")
            if name == self.basesymbol:
                assets["Stocks"] = float(item.get("free"))
                assets["FrozenStocks"] = float(item.get("locked"))
            if name == self.quotesymbol:
                assets["Balance"] = float(item.get("free"))
                assets["FrozenBalance"] = float(item.get("locked"))
        assets["Info"] = result
        self.assets = assets
    
    async def get_bookTicker_update(self, *args, **kwargs):
        result, error = await self.ba_rest_api.get_bookTicker(self.symbol)
        if error:
            logger.warn("platform:", self.platform, error, "get_bookTicker info failed!", caller=self)
            return
        self.ticker = result
        self.bidPrice = float(self.ticker["bidPrice"])
        self.askPrice = float(self.ticker["askPrice"])
        self.midprice = (self.bidPrice+self.askPrice)/2
    
    async def get_server_time_update(self, *args, **kwargs):
        result, error = await self.ba_rest_api.get_server_time()
        if error:
            logger.warn("platform:", self.platform, error, "get_server_time info failed!", caller=self)
            return
        self.servertime = int(result["serverTime"])
    
    @async_method_locker("buy",wait=False)
    async def buy(self,quantity,price):
        """买入下单"""
        balance = self.assets["Balance"]
        if quantity<=self.trade_info[self.symbol]["minQty"] or quantity*self.midprice<self.trade_info[self.symbol]['minvalues']:
            print("下单价值过小")
            #logger.info("下单价值过小",caller=self)
            return
        if balance<quantity*price:
            print("资产余额不足:", balance ,"需要:",round(quantity*price,3))

            #logger.info("资产余额不足:", balance ,"需要:",round(quantity*price,3),caller=self)
            return
        orderid, error = await self.trader.create_order(ORDER_ACTION_BUY,price,quantity,"LIMIT_MAKER","ACK")

        if error:
            print("下单买单失败:",quantity,price, error)
            #logger.info("下单买单失败:",quantity,price, error,caller=self)
        print("买单订单ID:", orderid)
        #logger.info("买单订单ID:", orderid,caller=self)
    
    @async_method_locker("sell",wait=False)
    async def sell(self,quantity,price):
        """卖出下单"""
        Stocks = self.assets['Stocks']
        if (quantity<=self.trade_info[self.symbol]["minQty"]) or quantity*self.midprice<self.trade_info[self.symbol]['minvalues']:
            print("卖出下单价值过小")
            #logger.info("卖出下单价值过小",caller=self)
            return
        if Stocks<quantity:
            print("资产余额不足:", Stocks ,"需要:",round(quantity,5))
            #logger.info("资产余额不足:", Stocks ,"需要:",round(quantity,5),caller=self)
            return
        orderid, error = await self.trader.create_order(ORDER_ACTION_SELL,price,quantity,"LIMIT_MAKER","ACK")

        if error:
            print("下单卖单失败:",quantity,price, error)
            #logger.info("下单卖单失败:",quantity,price, error,caller=self)
        print("卖单订单ID:", orderid)
        #logger.info("卖单订单ID:", orderid,caller=self)
    
    @async_method_locker("cancelorders",wait=False)
    async def cancelorders(self):
        """撤销订单"""
        success, error = await self.ba_rest_api.revoke_order(self.symbol)
        if error:
            print(error)
            return
        print(success)


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
