""" configfront:[{"variatename":"basesymbol","description":"交易币","remark":"交易币","type":"string","value":"DREP"},{"variatename":"quotesymbol","description":"报价币","remark":"报价币","type":"string","value":"BUSD"},{"variatename":"startbalance","description":"初始资产","remark":"","type":"number","value":500},{"variatename":"floatvaluesbuy","description":"买单深度","remark":"","type":"number","value":1000},{"variatename":"floatvaluessell","description":"卖单深度","remark":"","type":"number","value":1000},{"variatename":"diffprice","description":"盘口价差率","remark":"盘后价差率","type":"number","value":0.002},{"variatename":"mindiffpricerate","description":"最新盘口价差率","remark":"交易币","type":"number","value":0.001},{"variatename":"maxtradevalueschushi","description":"初始持仓量","remark":"初始持仓量","type":"number","value":50},{"variatename":"chushidepthvalue","description":"初始深度量","remark":"初始深度量","type":"number","value":300},{"variatename":"goontrade","description":"启动自动开启交易","remark":"启动自动开启交易","type":"boole","value":false},{"variatename":"lossvalue","description":"止损金额","remark":"止损金额","type":"number","value":10},{"variatename":"ordertype","description":"订单类型","remark":"订单类型","type":"selected","value":"只做挂单|限价单"},{"variatename":"robdish","description":"抢盘口模式","remark":"","type":"boole","value":"false"}]"""


#2022-08-01,加入锁，尝试解决运行一段时间堆栈溢出问题
#2022-07-21,清仓后资产统计错误修复
#2022-07-15 1、增加抢盘口模式

# -*- coding:utf-8 -*-

from email import header
from multiprocessing.connection import wait
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
from quant.asset import Asset

from quant.market import Orderbook
from quant.order import ORDER_ACTION_BUY, ORDER_STATUS_FAILED, ORDER_STATUS_CANCELED, ORDER_STATUS_FILLED,ORDER_ACTION_SELL
from quant.tasks import LoopRunTask,SingleTask
from quant.platform import binance
import math
from quant.utils.decorator import async_method_locker
import time
import asyncio
import nest_asyncio
from quant.utils import sqlite3db
import json
import requests


nest_asyncio.apply()

class Tymain:

    def __init__(self):
        """ 初始化
        """
        

        try:
            self.strategy = config.strategy
            self.platform = config.accounts[0]["platform"]
            self.account = config.accounts[0]["account"]
            self.access_key = config.accounts[0]["access_key"]
            self.secret_key = config.accounts[0]["secret_key"]
            self.username = config.username
            self.robotid = config.SERVER_ID
            self.tyquantipaddress = config.tyquantcentreserver.get("host", "120.26.56.32")
            self.tyquantport = config.tyquantcentreserver.get("port", "8088")

            self.host = "https://api.binance.com"
            self.ba_rest_api = binance.BinanceRestAPI(self.host,self.access_key,self.secret_key)            
            self.TY = sqlite3db.Sqlitedb(Logtocmd=True)
            
            #自定义参数
            self.basesymbol = config.strategyargs["basesymbol"].upper()
            self.quotesymbol = config.strategyargs["quotesymbol"].upper()
            self.symbol = self.basesymbol+self.quotesymbol
            self.startbalance = float(config.strategyargs["startbalance"])
            self.floatvaluesbuy = float(config.strategyargs["floatvaluesbuy"])
            self.floatvaluessell = float(config.strategyargs["floatvaluessell"])
            self.diffprice = float(config.strategyargs["diffprice"])
            self.mindiffpricerate = float(config.strategyargs["mindiffpricerate"])
            self.maxtradevalueschushi = float(config.strategyargs["maxtradevalueschushi"])
            self.chushidepthvalue = float(config.strategyargs["chushidepthvalue"])
            self.goontrade = config.strategyargs["goontrade"]
            self.lossvalue = float(config.strategyargs["lossvalue"])
            self.ordertype = ["LIMIT_MAKER","LIMIT"][int(config.strategyargs["ordertype"])]
            self.robdish = config.strategyargs["robdish"] #抢盘口模式

        except Exception as error:
            # 发生错误时回滚
            self.TY.Log(str(error))
            self.TY.Log("\n" + traceback.format_exc())

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
        self.profit = 0
        self.lossv = 0
        self.tickerdelay = 0
        self.tickertotradedelay = 0
        self.totalbalance = 0


        #self.renamerobot()
        self.TY.Log("策略初始化完成")
        

        #SingleTask.run(self.anyncgotest)

        #SingleTask.run(self.anyncgo)
        

        

        LoopRunTask.register(self.test,2)

        # 订阅行情
        #Market(const.MARKET_TYPE_ORDERBOOK, self.platform, self.symbol, self.mainloop)

        self.looptime = 0
      
        #SingleTask.run(self.anyncgo)
    
    #执行并发任务
    async def gotasks(self):
        tasks = [asyncio.create_task(self.asynUpdate_ExchangeInfo()), asyncio.create_task(self.asynUpdate_ExchangeInfo())]
        done,pending = await asyncio.wait(tasks)

    #加入锁测试解决堆栈溢出
    @async_method_locker("anyncgo",wait=False)
    async def anyncgo(self,*args, **kwargs):
        while True:
            self.TY.Log("000")
            time1 = time.time()*1000
            if (time1-self.looptime)<1100:
                self.TY.Log(time1-self.looptime)                
                continue
            asyncio.run(self.gotasks())
            timedis = int(time.time()*1000-self.looptime)
            self.TY.Log("轮训间隔(毫秒):",timedis)
            self.looptime = int(time.time()*1000)


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
    def cut_num(self,num, Precision):
        ret = math.floor(num*10**Precision) / 10**Precision
        return ret
    def Update_ExchangeInfo(self):  # 同步获取交易规则
        success, error ,header= self.ba_rest_api.synHttpQuery("GET","https://api.binance.com/api/v3/exchangeInfo",rethead=True)
        print("同步",error,header)

    
    async def asynUpdate_ExchangeInfo(self):  # 异步获取交易规则
        success, error ,header= await self.ba_rest_api.HttpQuery("GET","https://api.binance.com/api/v3/exchangeInfo",rethead=True)
        self.servertime = success["serverTime"]
        print("异步",error,header)


    def cancelorders0(self):
        """撤销订单"""
        try:
            params = {"symbol":self.symbol,"timestamp":tools.get_cur_timestamp_ms()}
            success, error, header = self.ba_rest_api.synexchangeIO("DELETE","/api/v3/openOrders",params,rethead=True)
            if error:
                self.TY.Log("同步撤销订单报错",error)
                return
            self.TY.Log("同步撤销订单",success,header)
        except Exception as error:
            # 发生错误时回滚
            self.TY.Log("同步撤销订单报错",str(error))
            self.TY.Log("\n" + traceback.format_exc())
        

    async def test(self,*args, **kwargs):
        #params = {"symbol":self.symbol,"limit":110}
        #success, error = await self.ba_rest_api.HttpQuery("GET","https://api.binance.com/api/v3/trades",params=params)
        #print(json.loads(success),type(json.loads(success)))
        #print(success,type(success))
        time0 = time.time()*1000

        await self.anyncgotest0()
        print(time.time()*1000-time0)



        



    #加入锁测试解决堆栈溢出,并发任务
    @async_method_locker("anyncgotest0",wait=False)
    async def anyncgotest0(self,*args, **kwargs):
        SingleTask.run(self.asynUpdate_ExchangeInfo)
        SingleTask.run(self.asynUpdate_ExchangeInfo)

    #加入锁测试解决堆栈溢出,并发任务
    @async_method_locker("anyncgotest",wait=True)
    async def anyncgotest(self,*args, **kwargs):
        time0 = time.time()*1000
        loop = asyncio.get_event_loop()
        tasks = [self.asynUpdate_ExchangeInfo(), self.asynUpdate_ExchangeInfo()]
        loop.run_until_complete(asyncio.gather(*tasks))
        #asyncio.run(await asyncio.wait(tasks))
        print(time.time()*1000-time0)

    


    async def mainloop(self,*args, **kwargs):
        """ 订单薄更新
        """
        #机器人循环状态参数，1为正常循环，2为进入tyonexit()
        try:
            self.robotloopcmd = config.robotloopcmd

            #自定义参数
            self.basesymbol = config.strategyargs["basesymbol"].upper()
            self.quotesymbol = config.strategyargs["quotesymbol"].upper()
            if (not self.symbol==self.basesymbol+self.quotesymbol):
                self.loopnum=0
            self.symbol = self.basesymbol+self.quotesymbol
            self.startbalance = float(config.strategyargs["startbalance"])
            self.floatvaluesbuy = float(config.strategyargs["floatvaluesbuy"])
            self.floatvaluessell = float(config.strategyargs["floatvaluessell"])
            self.diffprice = float(config.strategyargs["diffprice"])
            self.mindiffpricerate = float(config.strategyargs["mindiffpricerate"])
            self.maxtradevalueschushi = float(config.strategyargs["maxtradevalueschushi"])
            self.chushidepthvalue = float(config.strategyargs["chushidepthvalue"])
            if (not self.goontrade==config.strategyargs["goontrade"]):
                self.loopnum=0
            self.goontrade = config.strategyargs["goontrade"]
            self.lossvalue = float(config.strategyargs["lossvalue"])
            self.ordertype = ["LIMIT_MAKER","LIMIT"][int(config.strategyargs["ordertype"])]
            self.robdish = config.strategyargs["robdish"] #抢盘口模式
            


            self.TY.Log("机器人循环状态",self.robotloopcmd,self.symbol,caller = self)

            if self.robotloopcmd == 2:  #rabbitmq动态更新参数为退出循环
                await self.tyonexit()  

            if time.time()*1000-self.perlooptime<1.5*1000:
                return

            await self.cancelorders()            
            time0 = time.time()*1000
            await self.anyncgo()
            self.tickerdelay = time.time()*1000-time0
            self.TY.Log("self.tickerdelay",self.tickerdelay)
            if self.tickerdelay>1000:
                self.TY.Log("行情更新延迟高,该循环不处理交易")
                return
            orderbook=self.orderbook
            self.midprice = (float(orderbook["bids"][0][0])+float(orderbook["asks"][0][0]))/2
            account = self.assets

            if (self.loopnum==0):
                self.inittrade()
                self.TY.Log(self.symbol,"账户信息延迟:",  time.time()*1000-time0, caller=self)
                while 1:
                    await self.updateexinfo()
                    try:
                        if self.trade_info[self.symbol]['amountSize']>=0:
                            self.TY.Log("amountSize:", self.trade_info[self.symbol]['amountSize'], caller=self)
                            break
                        else:
                            self.TY.Log(self.symbol,"amountSize",self.trade_info[self.symbol]['amountSize'],caller=self)
                    except Exception as r:
                        print('未知错误 %s' %(r))
                    time.sleep(1)           
                self.initbalance = (account['Stocks']+account['FrozenStocks'])*self.midprice+account['Balance']+account['FrozenBalance']   
                self.TY.Log("Stocks:",self.assets["Stocks"], caller=self) 

            buyPrice = self.GetPrice("Buy",orderbook)
            sellPrice = self.GetPrice("Sell",orderbook)
            if self.islog:
                self.TY.Log("buyPrice:", buyPrice, caller=self)
                self.TY.Log("sellPrice:", sellPrice, caller=self)
            if ((sellPrice - buyPrice) <= self.diffprice / 100 * self.midprice and (not self.robdish)):
                buyPrice=float(orderbook["bids"][5][0])
                sellPrice=float(orderbook["asks"][5][0])
            maxtradevalues0=self.maxtradevalueschushi
            amountBuy = 0
            amountSell = 0
            if ((account['Stocks']+account['FrozenStocks'])*self.midprice>maxtradevalues0*2):
                amountBuy = 0
            else:
                amountBuy = self.cut_num(maxtradevalues0/buyPrice-(account['Stocks']+account['FrozenStocks']),self.trade_info[self.symbol]['amountSize'])
            amountSell = self.cut_num((account['Stocks']),self.trade_info[self.symbol]['amountSize']); 
            self.tickertotradedelay = 0
            if self.goontrade:
                self.tickertotradedelay = time.time()*1000-time0
                await self.buy(amountBuy,self.cut_num(buyPrice,self.trade_info[self.symbol]['priceSize']))
                await self.sell(amountSell,self.cut_num(sellPrice,self.trade_info[self.symbol]['priceSize']))
            self.totalbalance = (account['Stocks']+account['FrozenStocks'])*self.midprice+account['Balance']+account['FrozenBalance']
            self.profit = self.cut_num(self.totalbalance-self.startbalance,5)           
            self.TY.profit(self.cut_num(self.profit,5))          
            self.TY.profitreset(50000)
            if (self.totalbalance>self.initbalance):
                self.initbalance = self.totalbalance
            self.lossv = self.cut_num(self.totalbalance- self.initbalance,5)
            if (-self.lossv>self.lossvalue):
                #清仓并停止
                while (self.assets["Stocks"]+self.assets["FrozenStocks"])*self.bidPrice>self.trade_info[self.symbol]['minvalues']:
                    await self.cancelorders()
                    await self.sell(self.assets["Stocks"]+self.assets["FrozenStocks"],self.cut_num(self.askPrice,self.trade_info[self.symbol]['priceSize']))
                    time.sleep(2) 
                    await self.check_asset_update()
                    await self.get_orderbook_update()
                account = self.assets
                self.totalbalance = (account['Stocks']+account['FrozenStocks'])*self.midprice+account['Balance']+account['FrozenBalance']
                self.profit = self.totalbalance-self.startbalance
                self.TY.profit(self.cut_num(self.profit,5),"&")
                self.TY.Log("平仓撤单完成，机器人将停止")
                #self.TY.Log("平仓撤单完成，机器人将停止",caller = self)            
                quant.stop(username=self.username,robotid=self.robotid)
            self.updatestatus()
            self.perlooptime = time.time()*1000
            self.TY.Logreset(3000)
            self.loopnum = self.loopnum + 1
        except Exception as error:
            # 发生错误时回滚
            self.TY.Log(str(error))
            self.TY.Log("\n" + traceback.format_exc())

    def updatestatus(self):
        table = {
                "type": "table",
                "title": "账户信息",
                "cols": [
                    "序号",
                    "币种",
                    "可用数量",
                    "冻结数量",
                    "数量",
                ],
                "rows": [],
            }
        num = 1
        for asset in self.assets["Info"]["balances"]:
            coinamount = float(asset["free"])+float(asset["locked"])
            if coinamount>0:
                table["rows"].append([num,asset["asset"],float(asset["free"]),float(asset["locked"]),coinamount])
                num += 1
        logstr = "是否启动交易:"+str(self.goontrade)+"\n"+\
                "当前利润:"+str(self.profit)+"\n"+\
                "回撤金额:"+str(self.lossv)+"\n"+\
                "初始持仓量:"+str(self.maxtradevalueschushi)+"\n" +\
                "初始深度量:"+str(self.chushidepthvalue)+"\n" +\
                "是否抢盘口模式:"+str(self.robdish)+"\n" +\
                "行情更新延迟:"+str(self.tickerdelay)+"\n"+\
                "行情更新距下单:"+str(self.tickertotradedelay)+"\n"+\
                "折合总资产:"+str(self.totalbalance)+"\n"
        logstr = logstr+"`"+json.dumps([table])+"`\n"
        self.TY.updatestatus(logstr)  

    @async_method_locker("coverpos",wait=True)
    async def coverpos(self):
        #清仓并停止,吃单清仓
        while (self.assets["Stocks"]+self.assets["FrozenStocks"])*self.bidPrice>self.trade_info[self.symbol]['minvalues']:
            await self.cancelorders()
            await self.sell(self.assets["Stocks"]+self.assets["FrozenStocks"],self.cut_num(self.bidPrice,self.trade_info[self.symbol]['priceSize']))
            time.sleep(2) 
            await self.check_asset_update()
            await self.get_orderbook_update()
        self.TY.Log("清仓完成")       
        return

    @async_method_locker("tyonexit",wait=True)
    async def tyonexit(self):
        self.TY.Log("进入扫尾")       
        await self.coverpos()
        self.updatestatus()
        quant.stop(username=self.username,robotid=self.robotid)
    #重命名机器人
    def renamerobot(self):
        url = "http://"+self.tyquantipaddress+":"+self.tyquantport+"/updaterobotname/"
        data = {
            "username":self.username,
            "robotname":"简单高频机器人"+self.basesymbol+"_"+self.quotesymbol,
            "robotid":self.robotid,
        }
        res = requests.post(url=url,data=data)
        self.TY.Log(res.text)  
    #计算将要下单的价格
    def GetPrice(self,ptype,depth) :
        amountBidsvalues=0
        amountAsksvalues=0
        if(ptype=="Buy"):
            if self.robdish:
                return float(depth["bids"][0][0])+self.trade_info[self.symbol]["tickSize"]
            else:
                for i in range(20):
                    amountBidsvalues+=float(depth["bids"][i][0])*float(depth["bids"][i][1])
                    if (amountBidsvalues>self.floatvaluesbuy):
                        return float(depth["bids"][i][0])+self.trade_info[self.symbol]["tickSize"]
            return float(depth["bids"][0][0])
            
        if(ptype=="Sell"):
            if self.robdish:
                return float(depth["asks"][0][0])-self.trade_info[self.symbol]["tickSize"]
            else:
                for j in range(20):
                    amountAsksvalues+=float(depth["asks"][j][0])*float(depth["asks"][j][1])
                    if (amountAsksvalues>self.floatvaluessell):
                        return float(depth["asks"][j][0])-self.trade_info[self.symbol]["tickSize"]
            return float(depth["asks"][0][0])

    async def on_event_order_update(self, order: Order):
        """ 订单状态更新
        """
        self.TY.Log("order update:", order, caller=self)

        # 如果订单失败、订单取消、订单完成交易
        if order.status in [ORDER_STATUS_FAILED, ORDER_STATUS_CANCELED, ORDER_STATUS_FILLED]:
            pass
            #self.order_no = None

    async def on_event_asset_update(self, asset:Asset):
        """ 订单状态更新
        """
        self.TY.Log("asset update:", asset, caller=self)

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
            self.TY.Log(error,caller = self)
    
    async def get_orderbook_update (self,*args, **kwargs):
        success, error = await self.ba_rest_api.get_orderbook(self.symbol,limit = 20)
        if success:
            self.orderbook = success  
            self.bidPrice = float(self.orderbook["bids"][0][0])
            self.askPrice = float(self.orderbook["asks"][0][0])
            self.midprice = (self.bidPrice+self.askPrice)/2
            #logger.debug("get_orderbook_update",self.orderbook,caller = self)

        else:
            self.TY.Log("get_orderbook_update",error,caller = self)

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
    
    # async def get_bookTicker_update(self, *args, **kwargs):
    #     result, error = await self.ba_rest_api.get_bookTicker(self.symbol)
    #     if error:
    #         logger.warn("platform:", self.platform, error, "get_bookTicker info failed!", caller=self)
    #         return
    #     self.ticker = result
    #     self.bidPrice = float(self.ticker["bidPrice"])
    #     self.askPrice = float(self.ticker["askPrice"])
    #     self.midprice = (self.bidPrice+self.askPrice)/2
    
    
    @async_method_locker("buy",wait=False)
    async def buy(self,quantity,price):
        """买入下单"""
        balance = self.assets["Balance"]
        if quantity<=self.trade_info[self.symbol]["minQty"] or quantity*self.midprice<self.trade_info[self.symbol]['minvalues']:
            self.TY.Log("下单价值过小")
            #self.TY.Log("下单价值过小",caller=self)
            return
        if balance<quantity*price:
            self.TY.Log("资产余额不足:", balance ,"需要:",self.cut_num(quantity*price,3))

            #self.TY.Log("资产余额不足:", balance ,"需要:",self.cut_num(quantity*price,3),caller=self)
            return
        
        orderid, error = await self.trader.create_order(ORDER_ACTION_BUY,price,quantity,self.ordertype,"ACK")

        if error:
            self.TY.Log("下单买单失败:",quantity,price, error)
            #self.TY.Log("下单买单失败:",quantity,price, error,caller=self)
        self.TY.Log("买单订单ID:", orderid)
        #self.TY.Log("买单订单ID:", orderid,caller=self)
    
    @async_method_locker("sell",wait=False)
    async def sell(self,quantity,price):
        """卖出下单"""
        Stocks = self.assets['Stocks']
        if (quantity<=self.trade_info[self.symbol]["minQty"]) or quantity*self.midprice<self.trade_info[self.symbol]['minvalues']:
            self.TY.Log("卖出下单价值过小")
            #self.TY.Log("卖出下单价值过小",caller=self)
            return
        if Stocks<quantity:
            self.TY.Log("资产余额不足:", Stocks ,"需要:",self.cut_num(quantity,5))
            #self.TY.Log("资产余额不足:", Stocks ,"需要:",self.cut_num(quantity,5),caller=self)
            return
        orderid, error = await self.trader.create_order(ORDER_ACTION_SELL,price,quantity,self.ordertype,"ACK")

        if error:
            self.TY.Log("下单卖单失败:",quantity,price, error)
            #self.TY.Log("下单卖单失败:",quantity,price, error,caller=self)
        self.TY.Log("卖单订单ID:", orderid)
        #self.TY.Log("卖单订单ID:", orderid,caller=self)
    
    @async_method_locker("cancelorders",wait=False)
    async def cancelorders(self):
        """撤销订单"""
        success, error = await self.ba_rest_api.revoke_order(self.symbol)
        if error:
            self.TY.Log(error)
            return
        self.TY.Log(success)