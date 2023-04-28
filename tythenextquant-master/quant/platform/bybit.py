# -*- coding:utf-8 -*-

"""
Bybit.io Trade module.
https://Bybitio.news/api2#spot

Author: xunfeng
Date:   2019/07/15
Email:  xunfeng@test.com
"""

import copy
import hmac
import hashlib
import urllib
from urllib.parse import urljoin

from quant.error import Error
from quant.utils import logger
from quant.const import BYBIT
from quant.order import Order
from quant.asset import Asset, AssetSubscribe
from quant.tasks import SingleTask, LoopRunTask
from quant.utils.http_client import AsyncHttpRequests
from quant.utils.http_client import SyncHttpRequests
import time
import json
import math
import traceback

from quant.utils import tools
from quant.utils.decorator import async_method_locker
from quant.order import ORDER_TYPE_LIMIT, ORDER_ACTION_BUY, ORDER_ACTION_SELL
from quant.order import ORDER_STATUS_SUBMITTED, ORDER_STATUS_PARTIAL_FILLED, ORDER_STATUS_FILLED, \
    ORDER_STATUS_CANCELED, ORDER_STATUS_FAILED


__all__ = ("BybitRestAPI", "BybitTrade", )


class BybitRestAPIV5:
    """ Bybit V5 REST API client.
    统一账户
    https://api.bybit.com
    https://api.bytick.com

    Attributes:
        host: HTTP request host.
        access_key: Account's ACCESS KEY.
        secret_key Account's SECRET KEY.
    """

    def __init__(self, host, access_key, secret_key,platform=None,account=None):
        """initialize REST API client."""
        self._host = host
        self._platform = platform
        self._account = account
        self._access_key = access_key
        self._secret_key = secret_key
    
    async def GetExchangeInfo(self,symbol=None,autotry = False,sleep=100,reinfo = False):
        """ 获取交易所规则

        symbol:BTCUSDT
        autotry:自动重试
        sleep:休眠时间毫秒
        reinfo:是否返回原始信息

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if symbol:
            exsymbol = symbol.replace('_',"")
        else:
            success = {}
            error = "请输入要获取交易规则的交易对"
            logger.info(error,caller=self)
            return success, error
        successres=None
        while True:
            try:
                params = {"category": "spot"}
                exchangeInfo, error = await self.request("GET", "/v5/market/instruments-info", params)
                successres = exchangeInfo
                if exchangeInfo:
                    success={
                                "Info"    : exchangeInfo if reinfo else "",             #请求交易所接口后，交易所接口应答的原始数据
                                "minQty"    : 0,              #最小下单量
                                "maxQty"     : 0,               #最大下单量
                                "amountSize": 0,                    #数量精度位数
                                "priceSize"    : 0,               #价格精度位数
                                "tickSize"     : 0,               #单挑价格
                                "minNotional"    :0,               #最小订单名义价值
                                "Time"    : 0     #毫秒级别时间戳
                            }
                    for info in exchangeInfo["result"]["list"]:
                        if info["symbol"]==exsymbol:
                            minQty=float(info["lotSizeFilter"]["minOrderQty"])
                            maxQty=float(info["lotSizeFilter"]["maxOrderQty"])
                            amountSize = int((math.log10(1.1/float(info["lotSizeFilter"]["basePrecision"]))))
                            priceSize=int((math.log10(1.1/float(info["priceFilter"]["tickSize"]))))
                            tickSize = float(info["priceFilter"]["tickSize"])
                            minNotional = float(info["lotSizeFilter"]["minOrderAmt"]) #名义价值
                            success={
                                "Info"    : exchangeInfo if reinfo else "",             #请求交易所接口后，交易所接口应答的原始数据
                                "symbol":symbol,
                                "minQty"    : minQty,              #最小下单量
                                "maxQty"     : maxQty,               #最大下单量
                                "amountSize": amountSize,                    #数量精度位数
                                "priceSize"    : priceSize,               #价格精度位数
                                "tickSize"     : tickSize,               #单挑价格
                                "minNotional"    :minNotional,               #最小订单名义价值
                                "Time"    : successres["time"]      #毫秒级别时间戳
                            }
                            break
                    break
                else:
                    success={}
                if not autotry:
                    break
                else:
                    logger.info("交易所规范信息更新失败，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                    await tools.Sleep(sleep/1000)
            except Exception as e:
                success = {}
                if not autotry:
                    logger.info("交易所规范信息更新报错",successres,error,e,caller=self)
                    logger.info(traceback.format_exc())
                    break
                else:
                    logger.info("交易所规范信息更新报错，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                    logger.info(traceback.format_exc())
                    await tools.Sleep(sleep/1000)
        return success, error
    
    async def GetTicker(self,symbol=None,autotry = False,sleep=100):
        """ 获取当前交易对、合约对应的市场当前行情，返回值:Ticker结构体。

        symbol:BTCUSDT
        
        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if symbol:
            symbol = symbol.replace('_',"")
            params = {
                "category":"spot",
                "symbol":symbol
            }
        else:
            success = {}
            error = "请输入要获取Ticker的交易对"
            logger.info(error,caller=self)
            return success, error
        successres=None
        while True:
            try:
                success, error = await self.request("GET", "/v5/market/tickers", params)
                successres = success
                ticker=success["result"]
                if ticker["list"][0]:
                    success={
                        "Info"    : success,             #请求交易所接口后，交易所接口应答的原始数据，回测时无此属性
                        "High"    : float(ticker["list"][0]["highPrice24h"]),              #最高价，如果交易所接口没有提供24小时最高价则使用卖一价格填充
                        "Low"     : float(ticker["list"][0]["lowPrice24h"]),               #最低价，如果交易所接口没有提供24小时最低价则使用买一价格填充
                        "Sellamount": float(ticker["list"][0]["ask1Size"]),
                        "Sell"    : float(ticker["list"][0]["ask1Price"]),               #卖一价
                        "Buy"     : float(ticker["list"][0]["bid1Price"]),               #买一价
                        "Buyamount": float(ticker["list"][0]["bid1Size"]),
                        "Last"    : float(ticker["list"][0]["lastPrice"]),               #最后成交价
                        "Volume"  : float(ticker["list"][0]["volume24h"]),          #最近成交量
                        "Time"    : successres["time"]     #毫秒级别时间戳
                    }
                    break
                else:
                    success={}
                if not autotry:
                    break
                else:
                    logger.info("Ticker行情更新失败，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                    await tools.Sleep(sleep/1000)
            except Exception as e:
                success = {}
                if not autotry:
                    logger.info("Ticker行情更新报错",successres,error,e,caller=self)
                    break
                else:
                    logger.info("Ticker行情更新报错，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                    await tools.Sleep(sleep/1000)
        return success, error

    
    async def GetDepth(self,symbol=None,limit = 100,autotry = False,sleep=100):
        """ 获取当前交易对、合约对应的市场的订单薄数据，返回值：Depth结构体。

        symbol:BTCUSDT

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        ts = tools.get_cur_timestamp_ms()
        if symbol:
            if limit>50:
                limit=50
            symbol = symbol.replace('_',"")
            params = {
                "category":"spot",
                "symbol":symbol,
                "limit":limit
            }
        else:
            success = {}
            error = "请输入要获取depth的交易对"
            logger.info("请输入要获取depth的交易对",caller=self)
            return success, error
        successres=None
        while True:
            try:
                success, error = await self.request("GET", "/v5/market/orderbook", params)
                successres = success
                depth = success["result"]
                if depth:
                    Bids=[]
                    Asks=[]
                    for dp in depth["b"][0:limit]:
                        Bids.append({"Price":float(dp[0]),"Amount":float(dp[1])})
                    for dp in depth["a"][0:limit]:
                        Asks.append({"Price":float(dp[0]),"Amount":float(dp[1])})
                    success={
                        "Info"    : success,
                        "Asks"    : Asks,             #卖单数组，MarketOrder数组,按价格从低向高排序
                        "Bids"    : Bids,             #买单数组，MarketOrder数组,按价格从高向低排序
                        "Time"    : depth["ts"]      #毫秒级别时间戳
                    }
                    break
                else:
                    success={}
                if not autotry:
                    break
                else:
                    logger.info("depth行情更新失败，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                    await tools.Sleep(sleep/1000)
            except Exception as e:
                success = {}
                error = e
                if not autotry:
                    logger.info("depth行情更新报错",successres,error,e,caller=self)
                    break
                else:
                    logger.info("depth行情更新报错，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                    await tools.Sleep(sleep/1000)
        return success, error
    
    async def GetTrades(self,symbol=None,limit=60,autotry = False,sleep=100):
        """ 获取当前交易对、合约对应的市场的交易历史（非自己），返回值：Trade结构体数组

        symbol:BTCUSDT

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if symbol:
            symbol = symbol.replace('_',"")
            params = {
                "category":"spot",
                "symbol":symbol,
                "limit":limit
            }
        else:
            success = {}
            error = "请输入要获取Trade的交易对"
            logger.info("请输入要获取Trade的交易对",caller=self)
            return success, error
        successres=None
        while True:
            try:
                success, error = await self.request("GET", "/v5/market/recent-trade", params)
                successres = success
                trade = success["result"]["list"]
                if trade:
                    aggTrades = []
                    for tr in trade:
                        aggTrades.append({
                            "Id":tr["execId"],
                            "Time":tr["time"],
                            "Price":float(tr["price"]),
                            "Amount":float(tr["size"]),
                            "Type":0 if tr["side"]=="Buy" else 1,
                        })
                    success=aggTrades
                    break
                else:
                    success=[]
                if not autotry:
                    break
                else:
                    logger.info("Trade行情更新失败，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                    await tools.Sleep(sleep/1000)
            except Exception as e:
                success = []
                if not autotry:
                    logger.info("Trade行情更新报错",successres,error,e,caller=self)
                    break
                else:
                    logger.info("Trade行情更新报错，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                    await tools.Sleep(sleep/1000)
        return success, error

    async def GetAccount(self,basesymbol=None,quotesymbol=None,logrequestinfo=False,autotry = False,sleep=100):
        """ Get user account information.
        普通账户钱包信息

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        params = {"accountType":"UNIFIED"}
        successres=None
        if basesymbol and quotesymbol:
            while True:
                try:
                    success, error = await self.request("GET", "/v5/account/wallet-balance", params, auth=True,logrequestinfo=logrequestinfo)
                    successres = success
                    account = success["result"]["list"][0]
                    if account:
                        Stocks = 0
                        FrozenStocks = 0
                        Balance = 0
                        FrozenBalance = 0
                        for ac in account["coin"]:
                            if ac["coin"]==basesymbol:
                                Stocks=float(ac["availableToWithdraw"])
                                FrozenStocks=float(ac["walletBalance"])-float(ac["availableToWithdraw"])
                            if ac["coin"]==quotesymbol:
                                Balance=float(ac["availableToWithdraw"])
                                FrozenBalance=float(ac["walletBalance"])-float(ac["availableToWithdraw"])
                        success={
                            "Info"    : success,
                            "Balance"    : Balance,            
                            "FrozenBalance"    : FrozenBalance,             
                            "Stocks"    : Stocks,
                            "FrozenStocks"    : FrozenStocks
                        }
                        break
                    else:
                        success={}
                    if not autotry:
                        break
                    else:
                        logger.info("账户信息更新失败，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                        await tools.Sleep(sleep/1000)
                except Exception as e:
                    success = {}
                    if not autotry:
                        logger.info("账户信息更新报错",successres,error,e,caller=self)
                        break
                    else:
                        logger.info("账户信息更新报错，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                        await tools.Sleep(sleep/1000)
        else:
            success = {}
            error = "请输入要获取资产的交易对"
            logger.info("请输入要获取资产的交易对",caller=self)
        return success, error

    async def Buy(self, symbol, price, quantity,ttype="LIMIT",timeInForce="GTC",resptype="RESULT",logrequestinfo=False):
        """ Create an order.
        Args:
            symbol: Symbol name, e.g. BTCUSDT.
            price: Price of each contract.
            quantity: The buying or selling quantity.
            ttype: 订单类型.
            resptype: 订单信息响应类型，默认为RESULT字符串.(ACK,RESULT,FULL)
            timeInForce:GTC:成交为止,IOC:无法立即成交的部分就撤销,FOK:无法全部立即成交就撤销

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        symbol = symbol.replace('_',"")
        if ttype == "LIMIT":
            info = {
                "category":"spot",
                "symbol": symbol,
                "side": "Buy",
                "orderType": "Limit",
                "timeInForce": timeInForce,
                "qty": quantity,
                "price": price,
            }
        if ttype=="MARKET":
            info = {
                "category":"spot",
                "symbol": symbol,
                "side": "Buy",
                "orderType": "Market",
                "timeInForce": timeInForce,
                "qty": quantity,
                "price": price,
            }
        success, error = await self.request("POST", "/v5/order/create", body=info, auth=True,logrequestinfo=logrequestinfo)
        return success, error
    
    async def Sell(self, symbol, price, quantity,ttype="LIMIT",timeInForce="GTC",resptype="RESULT",logrequestinfo=False):
        """ Create an order.
        Args:
            symbol: Symbol name, e.g. BTCUSDT.
            price: Price of each contract.
            quantity: The buying or selling quantity.
            ttype: 订单类型.
            resptype: 订单信息响应类型，默认为RESULT字符串.(ACK,RESULT,FULL)
            timeInForce:GTC:成交为止,IOC:无法立即成交的部分就撤销,FOK:无法全部立即成交就撤销


        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        symbol = symbol.replace('_',"")
        if ttype == "LIMIT":
            info = {
                "category":"spot",
                "symbol": symbol,
                "side": "Sell",
                "orderType": "Limit",
                "timeInForce": timeInForce,
                "qty": quantity,
                "price": price,
            }
        if ttype=="MARKET":
            info = {
                "category":"spot",
                "symbol": symbol,
                "side": "Sell",
                "orderType": "Market",
                "timeInForce": timeInForce,
                "qty": quantity,
                "price": price,
            }
        success, error = await self.request("POST", "/v5/order/create", body=info, auth=True,logrequestinfo=logrequestinfo)
        return success, error

    async def CancelOrder(self, symbol, order_id = None, client_order_id = None):
        """ Cancelling an unfilled order.
        Args:
            symbol: Symbol name, e.g. BTCUSDT.
            order_id: Order id.
            client_order_id: Client order id.
            当没有传入订单ID时，撤销该交易对所有订单

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        symbol = symbol.replace('_',"")
        if order_id or client_order_id:
            params = {
                "category":"spot",
                "symbol": symbol,
                "orderId": str(order_id),
                "orderLinkId": str(client_order_id),
            }
            success, error = await self.request("POST", "/v5/order/cancel", params=params, auth=True)
        else:
            params = {
                "category":"spot",
                "symbol": symbol,
            }
            success, error = await self.request("POST", "/v5/order/cancel-all", params=params, auth=True)

        return success, error
    
    async def GetOrder(self, symbol, order_id = None,client_order_id=None,logrequestinfo=False):
        """ GetOrderID order.
        Args:
            symbol: Symbol name, e.g. BTCUSDT.
            order_id: Order id.
            client_order_id: Client order id.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if symbol:
            symbol = symbol.replace('_',"")

        else:
            success = {}
            error = "请输入交易对"
            logger.info("请输入交易对",caller=self)
            return success, error
        if order_id or client_order_id:
            pass
        else:
            success = {}
            error = "请输入交易ID"
            logger.info("请输入交易ID",caller=self)
            return success, error
        if order_id or client_order_id:
            params = {
                "category":"spot",
                "symbol": symbol,
                "orderId": str(order_id),
                "orderLinkId": str(client_order_id)
            }
            success, error = await self.request("GET", "/v5/order/realtime", params, auth=True,logrequestinfo=logrequestinfo )
            successres = success    
            data = success.get("result",{}).get("list",[])      
            if data[0]:
                if data[0]["orderStatus"]=="Cancelled" or data[0]["orderStatus"]=="PartiallyFilledCanceled":
                    status="已取消"
                elif data[0]["orderStatus"]=="Filled":
                    status="已完成"
                elif data[0]["orderStatus"]=="PartiallyFilled" or data[0]["orderStatus"]=="New" :
                    status="未完成"
                else:
                    status="其他" 
                try:
                    Price = float(data[0]["price"])
                except ValueError:
                    Price = 0.0    
                try:
                    Amount = float(data[0]["qty"])
                except ValueError:
                    Amount = 0.0 
                try:
                    DealAmount = float(data[0]["cumExecQty"])
                except ValueError:
                    DealAmount = 0.0 
                try:
                    AvgPrice = float(data[0]["avgPrice"])
                except ValueError:
                    AvgPrice = 0.0                    
                orderinfo = {
                    "Info"        : successres,         # 请求交易所接口后，交易所接口应答的原始数据，回测时无此属性
                    "Id"          : data[0]["orderId"],        # 交易单唯一标识
                    "Price"       : Price,          # 下单价格，注意市价单的该属性可能为0或者-1
                    "Amount"      : Amount,            # 下单数量，注意市价单的该属性可能为金额并非币数
                    "DealAmount"  : DealAmount,            # 成交数量，如果交易所接口不提供该数据则可能使用0填充
                    "AvgPrice"    : AvgPrice,          # 成交均价，注意有些交易所不提供该数据。不提供、也无法计算得出的情况该属性设置为0
                    "Status"      : status,             # 订单状态，参考常量里的订单状态，例如：ORDER_STATE_CLOSED
                    "Type"        : data[0]["side"],             # 订单类型，参考常量里的订单类型，例如：ORDER_TYPE_BUY
                    "Offset"      : 0,             # 数字货币期货的订单数据中订单的开平仓方向。ORDER_OFFSET_OPEN为开仓方向，ORDER_OFFSET_CLOSE为平仓方向
                    "ContractType" : ""            # 现货订单中该属性为""即空字符串，期货订单该属性为具体的合约代码
                }
            else:
                orderinfo={}       
            return orderinfo, error


    async def HttpQuery(self,method,url,params=None,data=None,rethead=False,logrequestinfo=False):
        """ 用户自定义HTTP请求接口

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if rethead:
            success, error , header= await self.hrequest(method, url,params,data=data,rethead = rethead,logrequestinfo=logrequestinfo)
            return success, error, header
        else:
            success, error = await self.hrequest(method, url,params,data=data,logrequestinfo=logrequestinfo)
            return success, error

    async def exchangeIO(self,method,uri, params,data=None,rethead=False,logrequestinfo=False):
        """用户自定义接口.

        Args:
            method:"GET" or "POST" or "DELETE"
            uri: /fapi/v1/ticker/price.
            params: 

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if rethead:
            success, error, header = await self.request(method, uri, params, data=data, auth=True,rethead=rethead,logrequestinfo=logrequestinfo)
            return success, error, header
        else:
            success, error = await self.request(method, uri, params, data=data, auth=True,logrequestinfo=logrequestinfo)
            return success, error

    async def request(self, method, uri, params=None, body=None, data=None, headers=None, auth=False, rethead=False,logrequestinfo=False):
        """ Do HTTP request.

        Args:
            method: HTTP request method. GET, POST, DELETE, PUT.
            uri: HTTP request uri.
            params: HTTP query params.
            body:   HTTP request body.
            headers: HTTP request headers.
            auth: If this request requires authentication.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        url = urljoin(self._host, uri)
        ts = str(tools.get_cur_timestamp_ms())
        pdata = {}
        recv_window=0
        if params:
            pdata.update(params)
            recv_window = params.get("recv_window")
            if recv_window:
                recv_window=str(recv_window)
            else:
                recv_window=0
        if body:
            pdata.update(body)

        if pdata:
            query = "&".join(["=".join([str(k), str(v)]) for k, v in pdata.items()])
        else:
            query = ""
        if method=="POST":
            query=json.dumps(pdata)
            data = query
        signature = ""
        if auth:
            if recv_window:
                param_str= ts + self._access_key +recv_window+ query
            else:
                param_str= ts + self._access_key + query
            signature = hmac.new(bytes(self._secret_key, "utf-8"), param_str.encode("utf-8"), hashlib.sha256).hexdigest()
        if query:
            if method=="POST":
                pass
            else:
                url += ("?" + query)
        if not headers:
            headers = {}
        headers["X-BAPI-API-KEY"] = self._access_key
        headers["X-BAPI-SIGN"] = signature
        headers["X-BAPI-TIMESTAMP"] = ts
        headers["Content-Type"] = "application/json"
        if recv_window:
            headers["X-BAPI-RECV-WINDOW"] = recv_window

        _header, success, error = await AsyncHttpRequests.fetch(method, url, body=data, headers=headers, timeout=10, verify_ssl=False,logrequestinfo=logrequestinfo)
        if rethead:
            return success, error, _header
        else:
            return success, error
    
    async def hrequest(self, method, url, params=None, body=None, data=None, headers=None, auth=False, rethead=False,logrequestinfo=False):
        """ Do HTTP request.

        Args:
            method: HTTP request method. GET, POST, DELETE, PUT.
            uri: HTTP request uri.
            params: HTTP query params.
            body:   HTTP request body.
            headers: HTTP request headers.
            auth: If this request requires authentication.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        pdata = {}
        if params:
            pdata.update(params)
        if body:
            pdata.update(body)

        if pdata:
            query = "&".join(["=".join([str(k), str(v)]) for k, v in pdata.items()])
        else:
            query = ""
        
        if query:
            url += ("?" + query)

        if not headers:
            headers = {}
        _header, success, error = await AsyncHttpRequests.fetch(method, url, body=data, headers=headers, timeout=10, verify_ssl=False,logrequestinfo=logrequestinfo)
        if rethead:
            return success, error, _header
        else:
            return success, error

    def synHttpQuery(self,method,url,params=None, data=None, rethead=False,logrequestinfo=False):
        """ 用户自定义httpt请求同步接口

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if rethead:
            success, error, header = self.synhrequest(method, url,params, data=data,rethead=rethead,logrequestinfo=logrequestinfo)
            return success, error, header
        else:
            success, error = self.synhrequest(method, url,params, data=data,logrequestinfo=logrequestinfo)
            return success, error

    def synexchangeIO(self,method,uri, params, data=None,rethead=False,logrequestinfo=False):
        """用户自定义同步交易所验证接口接口.

        Args:
            method:"GET" or "POST" or "DELETE"
            uri: /fapi/v1/ticker/price.
            params: 

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if rethead:
            success, error, header = self.synrequest(method, uri, params, data=data, auth=True,rethead=rethead,logrequestinfo=logrequestinfo)
            return success, error, header
        else:
            success, error = self.synrequest(method, uri, params, data=data, auth=True,logrequestinfo=logrequestinfo)
            return success, error

    #同步请求
    def synhrequest(self, method, url, params=None, body=None, data=None, headers=None, auth=False, rethead=False,logrequestinfo=False):
        """ Do HTTP request.

        Args:
            method: HTTP request method. GET, POST, DELETE, PUT.
            uri: HTTP request uri.
            params: HTTP query params.
            body:   HTTP request body.
            headers: HTTP request headers.
            auth: If this request requires authentication.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        pdata = {}
        if params:
            pdata.update(params)
        if body:
            pdata.update(body)

        if pdata:
            query = "&".join(["=".join([str(k), str(v)]) for k, v in pdata.items()])
        else:
            query = ""

        if query:
            url += ("?" + query)

        if not headers:
            headers = {}
        _headers, success, error = SyncHttpRequests.fetch(method, url, headers=headers, body=data, timeout=10, verify=False,logrequestinfo=logrequestinfo)
        if rethead:
            return success, error, _headers
        else:
            return success, error
    
    def synrequest(self, method, uri, params=None, body=None, data=None, headers=None, auth=False, rethead=False,logrequestinfo=False):
        """ Do HTTP request.

        Args:
            method: HTTP request method. GET, POST, DELETE, PUT.
            uri: HTTP request uri.
            params: HTTP query params.
            body:   HTTP request body.
            headers: HTTP request headers.
            auth: If this request requires authentication.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        url = urljoin(self._host, uri)
        ts = str(tools.get_cur_timestamp_ms())
        pdata = {}
        recv_window=0
        if params:
            pdata.update(params)
            recv_window = params.get("recv_window")
            if recv_window:
                recv_window=str(recv_window)
            else:
                recv_window=0
        if body:
            pdata.update(body)

        if pdata:
            query = "&".join(["=".join([str(k), str(v)]) for k, v in pdata.items()])
        else:
            query = ""
        if method=="POST":
            query=json.dumps(pdata)
            data = query
        signature = ""
        if auth:
            if recv_window:
                param_str= ts + self._access_key +recv_window+ query
            else:
                param_str= ts + self._access_key + query
            signature = hmac.new(bytes(self._secret_key, "utf-8"), param_str.encode("utf-8"), hashlib.sha256).hexdigest()
        if query:
            if method=="POST":
                pass
            else:
                url += ("?" + query)

        if not headers:
            headers = {}
        headers["X-BAPI-API-KEY"] = self._access_key
        headers["X-BAPI-SIGN"] = signature
        headers["X-BAPI-TIMESTAMP"] = ts
        headers["Content-Type"] = "application/json"
        if recv_window:
            headers["X-BAPI-RECV-WINDOW"] = recv_window

        _header, success, error = SyncHttpRequests.fetch(method, url, headers=headers, body=data, timeout=10, verify=False,logrequestinfo=logrequestinfo)
        if rethead:
            return success, error, _header
        else:
            return success, error


class BybitRestAPI:
    """ Bybit V3 REST API client.
    https://api.bybit.com
    https://api.bytick.com

    Attributes:
        host: HTTP request host.
        access_key: Account's ACCESS KEY.
        secret_key Account's SECRET KEY.
    """

    def __init__(self, host, access_key, secret_key,platform=None,account=None):
        """initialize REST API client."""
        self._host = host
        self._platform = platform
        self._account = account
        self._access_key = access_key
        self._secret_key = secret_key

    async def GetExchangeInfo(self,symbol=None,autotry = False,sleep=100,reinfo = False):
        """ 获取交易所规则

        symbol:BTCUSDT
        autotry:自动重试
        sleep:休眠时间毫秒
        reinfo:是否返回原始信息

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if symbol:
            exsymbol = symbol.replace('_',"")
        else:
            success = {}
            error = "请输入要获取交易规则的交易对"
            logger.info(error,caller=self)
            return success, error
        successres=None
        while True:
            try:
                exchangeInfo, error = await self.request("GET", "/spot/v3/public/symbols", "")
                successres = exchangeInfo
                if exchangeInfo:
                    success={
                                "Info"    : exchangeInfo if reinfo else "",             #请求交易所接口后，交易所接口应答的原始数据
                                "minQty"    : 0,              #最小下单量
                                "maxQty"     : 0,               #最大下单量
                                "amountSize": 0,                    #数量精度位数
                                "priceSize"    : 0,               #价格精度位数
                                "tickSize"     : 0,               #单挑价格
                                "minNotional"    :0,               #最小订单名义价值
                                "Time"    : 0     #毫秒级别时间戳
                            }
                    for info in exchangeInfo["result"]["list"]:
                        if info["name"]==exsymbol:
                            minQty=float(info["minTradeQty"])
                            maxQty=float(info["maxTradeQty"])
                            amountSize = int((math.log10(1.1/float(info["basePrecision"]))))
                            priceSize=int((math.log10(1.1/float(info["quotePrecision"]))))
                            tickSize = float(info["minPricePrecision"])
                            minNotional = float(info["minTradeAmt"]) #名义价值
                            success={
                                "Info"    : exchangeInfo if reinfo else "",             #请求交易所接口后，交易所接口应答的原始数据
                                "symbol":symbol,
                                "minQty"    : minQty,              #最小下单量
                                "maxQty"     : maxQty,               #最大下单量
                                "amountSize": amountSize,                    #数量精度位数
                                "priceSize"    : priceSize,               #价格精度位数
                                "tickSize"     : tickSize,               #单挑价格
                                "minNotional"    :minNotional,               #最小订单名义价值
                                "Time"    : successres["time"]      #毫秒级别时间戳
                            }
                            break
                    break
                else:
                    success={}
                if not autotry:
                    break
                else:
                    logger.info("交易所规范信息更新失败，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                    await tools.Sleep(sleep/1000)
            except Exception as e:
                success = {}
                if not autotry:
                    logger.info("交易所规范信息更新报错",successres,error,e,caller=self)
                    logger.info(traceback.format_exc())
                    break
                else:
                    logger.info("交易所规范信息更新报错，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                    logger.info(traceback.format_exc())
                    await tools.Sleep(sleep/1000)
        return success, error
    
    async def GetTicker(self,symbol=None,autotry = False,sleep=100):
        """ 获取当前交易对、合约对应的市场当前行情，返回值:Ticker结构体。

        symbol:BTCUSDT
        
        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if symbol:
            symbol = symbol.replace('_',"")
            params = {
                "symbol":symbol
            }
        else:
            success = {}
            error = "请输入要获取Ticker的交易对"
            logger.info(error,caller=self)
            return success, error
        successres=None
        while True:
            try:
                success, error = await self.request("GET", "/spot/v3/public/quote/ticker/24hr", params)
                successres = success
                ticker=success["result"]
                if ticker:
                    success={
                        "Info"    : success,             #请求交易所接口后，交易所接口应答的原始数据，回测时无此属性
                        "High"    : float(ticker["h"]),              #最高价，如果交易所接口没有提供24小时最高价则使用卖一价格填充
                        "Low"     : float(ticker["l"]),               #最低价，如果交易所接口没有提供24小时最低价则使用买一价格填充
                        "Sellamount": 0,
                        "Sell"    : float(ticker["ap"]),               #卖一价
                        "Buy"     : float(ticker["bp"]),               #买一价
                        "Buyamount": 0,
                        "Last"    : float(ticker["lp"]),               #最后成交价
                        "Volume"  : float(ticker["v"]),          #最近成交量
                        "Time"    : ticker["t"]     #毫秒级别时间戳
                    }
                    break
                else:
                    success={}
                if not autotry:
                    break
                else:
                    logger.info("Ticker行情更新失败，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                    await tools.Sleep(sleep/1000)
            except Exception as e:
                success = {}
                if not autotry:
                    logger.info("Ticker行情更新报错",successres,error,e,caller=self)
                    break
                else:
                    logger.info("Ticker行情更新报错，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                    await tools.Sleep(sleep/1000)
        return success, error

    
    async def GetDepth(self,symbol=None,limit = 100,autotry = False,sleep=100):
        """ 获取当前交易对、合约对应的市场的订单薄数据，返回值：Depth结构体。

        symbol:BTCUSDT

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        ts = tools.get_cur_timestamp_ms()
        if symbol:
            symbol = symbol.replace('_',"")
            params = {
                "symbol":symbol,
                "limit":limit
            }
        else:
            success = {}
            error = "请输入要获取depth的交易对"
            logger.info("请输入要获取depth的交易对",caller=self)
            return success, error
        successres=None
        while True:
            try:
                success, error = await self.request("GET", "/spot/v3/public/quote/depth", params)
                successres = success
                depth = success["result"]
                if depth:
                    Bids=[]
                    Asks=[]
                    for dp in depth["bids"][0:limit]:
                        Bids.append({"Price":float(dp[0]),"Amount":float(dp[1])})
                    for dp in depth["asks"][0:limit]:
                        Asks.append({"Price":float(dp[0]),"Amount":float(dp[1])})
                    success={
                        "Info"    : success,
                        "Asks"    : Asks,             #卖单数组，MarketOrder数组,按价格从低向高排序
                        "Bids"    : Bids,             #买单数组，MarketOrder数组,按价格从高向低排序
                        "Time"    : success["time"]      #毫秒级别时间戳
                    }
                    break
                else:
                    success={}
                if not autotry:
                    break
                else:
                    logger.info("depth行情更新失败，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                    await tools.Sleep(sleep/1000)
            except Exception as e:
                success = {}
                error = e
                if not autotry:
                    logger.info("depth行情更新报错",successres,error,e,caller=self)
                    break
                else:
                    logger.info("depth行情更新报错，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                    await tools.Sleep(sleep/1000)
        return success, error
    
    async def GetTrades(self,symbol=None,limit=60,autotry = False,sleep=100):
        """ 获取当前交易对、合约对应的市场的交易历史（非自己），返回值：Trade结构体数组

        symbol:BTCUSDT

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if symbol:
            symbol = symbol.replace('_',"")
            params = {
                "symbol":symbol,
                "limit":limit
            }
        else:
            success = {}
            error = "请输入要获取Trade的交易对"
            logger.info("请输入要获取Trade的交易对",caller=self)
            return success, error
        successres=None
        while True:
            try:
                success, error = await self.request("GET", "/spot/v3/public/quote/trades", params)
                successres = success
                trade = success["result"]["list"]
                if trade:
                    aggTrades = []
                    for tr in trade:
                        aggTrades.append({
                            "Id":None,
                            "Time":tr["time"],
                            "Price":float(tr["price"]),
                            "Amount":float(tr["qty"]),
                            "Type":0 if tr["isBuyerMaker"] else 1,
                        })
                    success=aggTrades
                    break
                else:
                    success=[]
                if not autotry:
                    break
                else:
                    logger.info("Trade行情更新失败，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                    await tools.Sleep(sleep/1000)
            except Exception as e:
                success = []
                if not autotry:
                    logger.info("Trade行情更新报错",successres,error,e,caller=self)
                    break
                else:
                    logger.info("Trade行情更新报错，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                    await tools.Sleep(sleep/1000)
        return success, error

    async def GetAccount(self,basesymbol=None,quotesymbol=None,logrequestinfo=False,autotry = False,sleep=100):
        """ Get user account information.
        普通账户钱包信息

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        params = ''
        successres=None
        if basesymbol and quotesymbol:
            while True:
                try:
                    success, error = await self.request("GET", "/spot/v3/private/account", params, auth=True,logrequestinfo=logrequestinfo)
                    successres = success
                    account = success["result"]
                    if account:
                        Stocks = 0
                        FrozenStocks = 0
                        Balance = 0
                        FrozenBalance = 0
                        for ac in account["balances"]:
                            if ac["coin"]==basesymbol:
                                Stocks=float(ac["free"])
                                FrozenStocks=float(ac["locked"])
                            if ac["coin"]==quotesymbol:
                                Balance=float(ac["free"])
                                FrozenBalance=float(ac["locked"])
                        success={
                            "Info"    : success,
                            "Balance"    : Balance,            
                            "FrozenBalance"    : FrozenBalance,             
                            "Stocks"    : Stocks,
                            "FrozenStocks"    : FrozenStocks
                        }
                        break
                    else:
                        success={}
                    if not autotry:
                        break
                    else:
                        logger.info("账户信息更新失败，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                        await tools.Sleep(sleep/1000)
                except Exception as e:
                    success = {}
                    if not autotry:
                        logger.info("账户信息更新报错",successres,error,e,caller=self)
                        break
                    else:
                        logger.info("账户信息更新报错，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                        await tools.Sleep(sleep/1000)
        else:
            success = {}
            error = "请输入要获取资产的交易对"
            logger.info("请输入要获取资产的交易对",caller=self)
        return success, error

    async def Buy(self, symbol, price, quantity,ttype="LIMIT",timeInForce="GTC",resptype="RESULT",logrequestinfo=False):
        """ Create an order.
        Args:
            symbol: Symbol name, e.g. BTCUSDT.
            price: Price of each contract.
            quantity: The buying or selling quantity.
            ttype: 订单类型.
            resptype: 订单信息响应类型，默认为RESULT字符串.(ACK,RESULT,FULL)
            timeInForce:GTC:成交为止,IOC:无法立即成交的部分就撤销,FOK:无法全部立即成交就撤销

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        symbol = symbol.replace('_',"")
        if ttype == "LIMIT":
            info = {
                "symbol": symbol,
                "side": "BUY",
                "orderType": "LIMIT",
                "timeInForce": timeInForce,
                "orderQty": quantity,
                "orderPrice": price,
            }
        else:
            if ttype=="LIMIT_MAKER":
                info = {
                    "symbol": symbol,
                    "side": "BUY",
                    "orderType": "LIMIT",
                    "timeInForce": timeInForce,
                    "orderQty": quantity,
                    "orderPrice": price,
                }
        success, error = await self.request("POST", "/spot/v3/private/order", body=info, auth=True,logrequestinfo=logrequestinfo)
        return success, error
    
    async def Sell(self, symbol, price, quantity,ttype="LIMIT",timeInForce="GTC",resptype="RESULT",logrequestinfo=False):
        """ Create an order.
        Args:
            symbol: Symbol name, e.g. BTCUSDT.
            price: Price of each contract.
            quantity: The buying or selling quantity.
            ttype: 订单类型.
            resptype: 订单信息响应类型，默认为RESULT字符串.(ACK,RESULT,FULL)
            timeInForce:GTC:成交为止,IOC:无法立即成交的部分就撤销,FOK:无法全部立即成交就撤销


        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        symbol = symbol.replace('_',"")
        if ttype == "LIMIT":
            info = {
                "symbol": symbol,
                "side": "SELL",
                "orderType": "LIMIT",
                "timeInForce": timeInForce,
                "orderQty": quantity,
                "orderPrice": price,
            }
        else:
            if ttype=="LIMIT_MAKER":
                info = {
                    "symbol": symbol,
                    "side": "SELL",
                    "orderType": "LIMIT_MAKER",
                    "timeInForce": timeInForce,
                    "orderQty": quantity,
                    "orderPrice": price,
                }
        success, error = await self.request("POST", "/spot/v3/private/order", body=info, auth=True,logrequestinfo=logrequestinfo)
        return success, error

    async def CancelOrder(self, symbol, order_id = None, client_order_id = None):
        """ Cancelling an unfilled order.
        Args:
            symbol: Symbol name, e.g. BTCUSDT.
            order_id: Order id.
            client_order_id: Client order id.
            当没有传入订单ID时，撤销该交易对所有订单

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        symbol = symbol.replace('_',"")
        if order_id or client_order_id:
            params = {
                "orderId": str(order_id),
                "orderLinkId": client_order_id,
            }
            success, error = await self.request("POST", "/spot/v3/private/cancel-order", params=params, auth=True)
        else:
            params = {
                "symbol": symbol,
                "orderTypes":"LIMIT,LIMIT_MAKER",
            }
            success, error = await self.request("POST", "/spot/v3/private/cancel-orders", params=params, auth=True)

        return success, error
    
    async def GetOrder(self, symbol, order_id = None,client_order_id=None,logrequestinfo=False):
        """ GetOrderID order.
        Args:
            symbol: Symbol name, e.g. BTCUSDT.
            order_id: Order id.
            client_order_id: Client order id.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if symbol:
            symbol = symbol.replace('_',"")

        else:
            success = {}
            error = "请输入交易对"
            logger.info("请输入交易对",caller=self)
            return success, error
        if order_id or client_order_id:
            pass
        else:
            success = {}
            error = "请输入交易ID"
            logger.info("请输入交易ID",caller=self)
            return success, error
        if order_id or client_order_id:
            params = {
                "symbol": symbol,
                "orderId": str(order_id),
                "orderLinkId": str(client_order_id)
            }
            success, error = await self.request("GET", "/spot/v3/private/order", params, auth=True,logrequestinfo=logrequestinfo )
            successres = success    
            data = success.get("result",{})    
            if data:
                if data["status"]=="CANCELED":
                    status="已取消"
                elif data["status"]=="FILLED":
                    status="已完成"
                elif data["status"]=="PARTIALLY_FILLED" or data["status"]=="NEW" :
                    status="未完成"
                else:
                    status="其他"              
                orderinfo = {
                    "Info"        : successres,         # 请求交易所接口后，交易所接口应答的原始数据，回测时无此属性
                    "Id"          : data["orderId"],        # 交易单唯一标识
                    "Price"       : float(data["orderPrice"]),          # 下单价格，注意市价单的该属性可能为0或者-1
                    "Amount"      : float(data["orderQty"]),            # 下单数量，注意市价单的该属性可能为金额并非币数
                    "DealAmount"  : float(data["execQty"]),            # 成交数量，如果交易所接口不提供该数据则可能使用0填充
                    "AvgPrice"    : float(data["avgPrice"]),          # 成交均价，注意有些交易所不提供该数据。不提供、也无法计算得出的情况该属性设置为0
                    "Status"      : status,             # 订单状态，参考常量里的订单状态，例如：ORDER_STATE_CLOSED
                    "Type"        :"Sell" if data["side"] == "SELL" else "Buy",             # 订单类型，参考常量里的订单类型，例如：ORDER_TYPE_BUY
                    "Offset"      : 0,             # 数字货币期货的订单数据中订单的开平仓方向。ORDER_OFFSET_OPEN为开仓方向，ORDER_OFFSET_CLOSE为平仓方向
                    "ContractType" : ""            # 现货订单中该属性为""即空字符串，期货订单该属性为具体的合约代码
                }
            else:
                orderinfo={}       
            return orderinfo, error


    async def HttpQuery(self,method,url,params=None,data=None,rethead=False,logrequestinfo=False):
        """ 用户自定义HTTP请求接口

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if rethead:
            success, error , header= await self.hrequest(method, url,params,data=data,rethead = rethead,logrequestinfo=logrequestinfo)
            return success, error, header
        else:
            success, error = await self.hrequest(method, url,params,data=data,logrequestinfo=logrequestinfo)
            return success, error

    async def exchangeIO(self,method,uri, params,data=None,rethead=False,logrequestinfo=False):
        """用户自定义接口.

        Args:
            method:"GET" or "POST" or "DELETE"
            uri: /fapi/v1/ticker/price.
            params: 

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if rethead:
            success, error, header = await self.request(method, uri, params, data=data, auth=True,rethead=rethead,logrequestinfo=logrequestinfo)
            return success, error, header
        else:
            success, error = await self.request(method, uri, params, data=data, auth=True,logrequestinfo=logrequestinfo)
            return success, error

    async def request(self, method, uri, params=None, body=None, data=None, headers=None, auth=False, rethead=False,logrequestinfo=False):
        """ Do HTTP request.

        Args:
            method: HTTP request method. GET, POST, DELETE, PUT.
            uri: HTTP request uri.
            params: HTTP query params.
            body:   HTTP request body.
            headers: HTTP request headers.
            auth: If this request requires authentication.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        url = urljoin(self._host, uri)
        ts = str(tools.get_cur_timestamp_ms())
        pdata = {}
        recv_window=0
        if params:
            pdata.update(params)
            recv_window = params.get("recv_window")
            if recv_window:
                recv_window=str(recv_window)
            else:
                recv_window=0
        if body:
            pdata.update(body)

        if pdata:
            query = "&".join(["=".join([str(k), str(v)]) for k, v in pdata.items()])
        else:
            query = ""
        if method=="POST":
            query=json.dumps(pdata)
            data = query
        signature = ""
        if auth:
            if recv_window:
                param_str= ts + self._access_key +recv_window+ query
            else:
                param_str= ts + self._access_key + query
            signature = hmac.new(bytes(self._secret_key, "utf-8"), param_str.encode("utf-8"), hashlib.sha256).hexdigest()
        if query:
            if method=="POST":
                pass
            else:
                url += ("?" + query)
        if not headers:
            headers = {}
        headers["X-BAPI-API-KEY"] = self._access_key
        headers["X-BAPI-SIGN"] = signature
        headers["X-BAPI-TIMESTAMP"] = ts
        headers["Content-Type"] = "application/json"
        if recv_window:
            headers["X-BAPI-RECV-WINDOW"] = recv_window

        _header, success, error = await AsyncHttpRequests.fetch(method, url, body=data, headers=headers, timeout=10, verify_ssl=False,logrequestinfo=logrequestinfo)
        if rethead:
            return success, error, _header
        else:
            return success, error
    
    async def hrequest(self, method, url, params=None, body=None, data=None, headers=None, auth=False, rethead=False,logrequestinfo=False):
        """ Do HTTP request.

        Args:
            method: HTTP request method. GET, POST, DELETE, PUT.
            uri: HTTP request uri.
            params: HTTP query params.
            body:   HTTP request body.
            headers: HTTP request headers.
            auth: If this request requires authentication.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        pdata = {}
        if params:
            pdata.update(params)
        if body:
            pdata.update(body)

        if pdata:
            query = "&".join(["=".join([str(k), str(v)]) for k, v in pdata.items()])
        else:
            query = ""
        
        if query:
            url += ("?" + query)

        if not headers:
            headers = {}
        _header, success, error = await AsyncHttpRequests.fetch(method, url, body=data, headers=headers, timeout=10, verify_ssl=False,logrequestinfo=logrequestinfo)
        if rethead:
            return success, error, _header
        else:
            return success, error

    def synHttpQuery(self,method,url,params=None, data=None, rethead=False,logrequestinfo=False):
        """ 用户自定义httpt请求同步接口

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if rethead:
            success, error, header = self.synhrequest(method, url,params, data=data,rethead=rethead,logrequestinfo=logrequestinfo)
            return success, error, header
        else:
            success, error = self.synhrequest(method, url,params, data=data,logrequestinfo=logrequestinfo)
            return success, error

    def synexchangeIO(self,method,uri, params, data=None,rethead=False,logrequestinfo=False):
        """用户自定义同步交易所验证接口接口.

        Args:
            method:"GET" or "POST" or "DELETE"
            uri: /fapi/v1/ticker/price.
            params: 

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if rethead:
            success, error, header = self.synrequest(method, uri, params, data=data, auth=True,rethead=rethead,logrequestinfo=logrequestinfo)
            return success, error, header
        else:
            success, error = self.synrequest(method, uri, params, data=data, auth=True,logrequestinfo=logrequestinfo)
            return success, error

    #同步请求
    def synhrequest(self, method, url, params=None, body=None, data=None, headers=None, auth=False, rethead=False,logrequestinfo=False):
        """ Do HTTP request.

        Args:
            method: HTTP request method. GET, POST, DELETE, PUT.
            uri: HTTP request uri.
            params: HTTP query params.
            body:   HTTP request body.
            headers: HTTP request headers.
            auth: If this request requires authentication.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        pdata = {}
        if params:
            pdata.update(params)
        if body:
            pdata.update(body)

        if pdata:
            query = "&".join(["=".join([str(k), str(v)]) for k, v in pdata.items()])
        else:
            query = ""

        if query:
            url += ("?" + query)

        if not headers:
            headers = {}
        _headers, success, error = SyncHttpRequests.fetch(method, url, headers=headers, body=data, timeout=10, verify=False,logrequestinfo=logrequestinfo)
        if rethead:
            return success, error, _headers
        else:
            return success, error
    
    def synrequest(self, method, uri, params=None, body=None, data=None, headers=None, auth=False, rethead=False,logrequestinfo=False):
        """ Do HTTP request.

        Args:
            method: HTTP request method. GET, POST, DELETE, PUT.
            uri: HTTP request uri.
            params: HTTP query params.
            body:   HTTP request body.
            headers: HTTP request headers.
            auth: If this request requires authentication.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        url = urljoin(self._host, uri)
        ts = str(tools.get_cur_timestamp_ms())
        pdata = {}
        recv_window=0
        if params:
            pdata.update(params)
            recv_window = params.get("recv_window")
            if recv_window:
                recv_window=str(recv_window)
            else:
                recv_window=0
        if body:
            pdata.update(body)

        if pdata:
            query = "&".join(["=".join([str(k), str(v)]) for k, v in pdata.items()])
        else:
            query = ""
        if method=="POST":
            query=json.dumps(pdata)
            data = query
        signature = ""
        if auth:
            if recv_window:
                param_str= ts + self._access_key +recv_window+ query
            else:
                param_str= ts + self._access_key + query
            signature = hmac.new(bytes(self._secret_key, "utf-8"), param_str.encode("utf-8"), hashlib.sha256).hexdigest()
        if query:
            if method=="POST":
                pass
            else:
                url += ("?" + query)

        if not headers:
            headers = {}
        headers["X-BAPI-API-KEY"] = self._access_key
        headers["X-BAPI-SIGN"] = signature
        headers["X-BAPI-TIMESTAMP"] = ts
        headers["Content-Type"] = "application/json"
        if recv_window:
            headers["X-BAPI-RECV-WINDOW"] = recv_window

        _header, success, error = SyncHttpRequests.fetch(method, url, headers=headers, body=data, timeout=10, verify=False,logrequestinfo=logrequestinfo)
        if rethead:
            return success, error, _header
        else:
            return success, error

class BybitTrade:
    """ Bybit.io Trade module. You can initialize trade object with some attributes in kwargs.

    Attributes:
        account: Account name for this trade exchange.
        strategy: What's name would you want to created for you strategy.
        symbol: Symbol name for your trade.
        host: HTTP request host. (default is "https://api.Bybitio.co")
        access_key: Account's ACCESS KEY.
        secret_key Account's SECRET KEY.
        asset_update_callback: You can use this param to specific a async callback function when you initializing Trade
            object. `asset_update_callback` is like `async def on_asset_update_callback(asset: Asset): pass` and this
            callback function will be executed asynchronous when received AssetEvent.
        order_update_callback: You can use this param to specific a async callback function when you initializing Trade
            object. `order_update_callback` is like `async def on_order_update_callback(order: Order): pass` and this
            callback function will be executed asynchronous when some order state updated.
        init_success_callback: You can use this param to specific a async callback function when you initializing Trade
            object. `init_success_callback` is like `async def on_init_success_callback(success: bool, error: Error, **kwargs): pass`
            and this callback function will be executed asynchronous after Trade module object initialized successfully.
        check_order_interval: The interval time(seconds) for loop run task to check order status. (default is 2 seconds)
    """

    def __init__(self, **kwargs):
        """Initialize."""
        e = None
        if not kwargs.get("account"):
            e = Error("param account miss")
        if not kwargs.get("strategy"):
            e = Error("param strategy miss")
        if not kwargs.get("symbol"):
            e = Error("param symbol miss")
        if not kwargs.get("host"):
            kwargs["host"] = "https://api.bybit.com"
        if not kwargs.get("access_key"):
            e = Error("param access_key miss")
        if not kwargs.get("secret_key"):
            e = Error("param secret_key miss")
        if e:
            logger.error(e, caller=self)
            if kwargs.get("init_success_callback"):
                SingleTask.run(kwargs["init_success_callback"], False, e)
            return

        self._account = kwargs["account"]
        self._strategy = kwargs["strategy"]
        self._platform = BYBIT
        self._symbol = kwargs["symbol"]
        self._host = kwargs["host"]
        self._access_key = kwargs["access_key"]
        self._secret_key = kwargs["secret_key"]
        self._asset_update_callback = kwargs.get("asset_update_callback")
        self._order_update_callback = kwargs.get("order_update_callback")
        self._init_success_callback = kwargs.get("init_success_callback")
        self._check_order_interval = kwargs.get("check_order_interval", 2)

        self._raw_symbol = self._symbol.replace("/", "_").lower()  # Raw symbol name for Exchange platform.

        self._assets = {}  # Asset information. e.g. {"BTC": {"free": "1.1", "locked": "2.2", "total": "3.3"}, ... }
        self._orders = {}  # Order details. e.g. {order_no: order-object, ... }

        # Initialize our REST API client.
        self._rest_api = BybitRestAPI(self._host, self._access_key, self._secret_key)

        # Create a loop run task to check order status.
        LoopRunTask.register(self._check_order_update, self._check_order_interval)

        # Subscribe asset event.
        if self._asset_update_callback:
            AssetSubscribe(self._platform, self._account, self.on_event_asset_update)

        SingleTask.run(self._initialize)

    @property
    def assets(self):
        return copy.copy(self._assets)

    @property
    def orders(self):
        return copy.copy(self._orders)

    @property
    def rest_api(self):
        return self._rest_api

    async def _initialize(self):
        """ Initialize. fetch all open order information."""
        result, error = await self._rest_api.get_open_orders(self._raw_symbol)
        if error:
            e = Error("get open order nos failed: {}".format(error))
            logger.error(e, caller=self)
            if self._init_success_callback:
                SingleTask.run(self._init_success_callback, False, e)
            return
        for order_info in result["orders"]:
            await self._update_order(order_info)
        if self._init_success_callback:
            SingleTask.run(self._init_success_callback, True, None)

    async def create_order(self, action, price, quantity, order_type=ORDER_TYPE_LIMIT, **kwargs):
        """ Create an order.

        Args:
            action: Trade direction, BUY or SELL.
            price: Price of order.
            quantity: The buying or selling quantity.
            order_type: order type, MARKET or LIMIT.

        Returns:
            order_no: Order ID if created successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        success, error = await self._rest_api.create_order(action, self._raw_symbol, price, quantity)
        if error:
            return None, error
        if not success["result"]:
            return None, success
        order_no = str(success["orderNumber"])
        infos = {
            "account": self._account,
            "platform": self._platform,
            "strategy": self._strategy,
            "order_no": order_no,
            "symbol": self._symbol,
            "action": action,
            "price": price,
            "quantity": quantity,
            "order_type": order_type
        }
        order = Order(**infos)
        self._orders[order_no] = order
        if self._order_update_callback:
            SingleTask.run(self._order_update_callback, copy.copy(order))
        return order_no, None

    async def revoke_order(self, *order_nos):
        """ Revoke (an) order(s).

        Args:
            order_nos: Order id list, you can set this param to 0 or multiple items. If you set 0 param, you can cancel
                all orders for this symbol(initialized in Trade object). If you set 1 param, you can cancel an order.
                If you set multiple param, you can cancel multiple orders. Do not set param length more than 100.

        Returns:
            Success or error, see bellow.
        """
        # If len(order_nos) == 0, you will cancel all orders for this symbol(initialized in Trade object).
        if len(order_nos) == 0:
            success, error = await self._rest_api.revoke_orders_all(self._raw_symbol)
            if error:
                return False, error
            if not success["result"]:
                return False, success
            return True, None

        # If len(order_nos) == 1, you will cancel an order.
        if len(order_nos) == 1:
            success, error = await self._rest_api.revoke_order(self._raw_symbol, order_nos[0])
            if error:
                return order_nos[0], error
            if not success["result"]:
                return False, success
            else:
                return order_nos[0], None

        # If len(order_nos) > 1, you will cancel multiple orders.
        if len(order_nos) > 1:
            success, error = await self._rest_api.revoke_orders(self._raw_symbol, order_nos)
            if error:
                return False, error
            if not success["result"]:
                return False, success
            return True, None

    async def get_open_order_nos(self):
        """ Get open order id list.

        Args:
            None.

        Returns:
            order_nos: Open order id list, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        success, error = await self._rest_api.get_open_orders(self._raw_symbol)
        if error:
            return False, error
        if not success["result"]:
            return False, success
        order_nos = []
        for order_info in success["orders"]:
            order_nos.append(str(order_info["orderNumber"]))
        return order_nos, None

    async def _check_order_update(self, *args, **kwargs):
        """ Loop run task for check order status.
        """
        order_nos = list(self._orders.keys())
        if not order_nos:
            return
        for order_no in order_nos:
            success, error = await self._rest_api.get_order_status(self._raw_symbol, order_no)
            if error or not success["result"]:
                return
            await self._update_order(success["order"])

    @async_method_locker("BybitTrade.order.locker")
    async def _update_order(self, order_info):
        """ Update order object.

        Args:
            order_info: Order information.
        """
        if not order_info:
            return
        status_updated = False
        order_no = str(order_info["orderNumber"])
        state = order_info["status"]

        order = self._orders.get(order_no)
        if not order:
            info = {
                "platform": self._platform,
                "account": self._account,
                "strategy": self._strategy,
                "order_no": order_no,
                "action": ORDER_ACTION_BUY if order_info["type"] == "buy" else ORDER_ACTION_SELL,
                "symbol": self._symbol,
                "price": order_info["rate"],
                "quantity": order_info["amount"],
                "remain": order_info["amount"],
                "avg_price": order_info["filledRate"]
            }
            order = Order(**info)
            self._orders[order_no] = order

        if state == "open":
            filled_amount = float(order_info["filledAmount"])
            if filled_amount == 0:
                state = ORDER_STATUS_SUBMITTED
                if order.status != state:
                    order.status = ORDER_STATUS_SUBMITTED
                    status_updated = True
            else:
                remain = float(order.quantity) - filled_amount
                if order.remain != remain:
                    order.status = ORDER_STATUS_PARTIAL_FILLED
                    order.remain = remain
                    status_updated = True
        elif state == "closed":
            order.status = ORDER_STATUS_FILLED
            order.remain = 0
            status_updated = True
        elif state == "cancelled":
            order.status = ORDER_STATUS_CANCELED
            filled_amount = float(order_info["filledAmount"])
            remain = float(order.quantity) - filled_amount
            if order.remain != remain:
                order.remain = remain
            status_updated = True
        else:
            logger.warn("state error! order_info:", order_info, caller=self)
            return

        if status_updated:
            order.avg_price = order_info["filledRate"]
            order.ctime = int(order_info["timestamp"] * 1000)
            order.utime = int(order_info["timestamp"] * 1000)
            if self._order_update_callback:
                SingleTask.run(self._order_update_callback, copy.copy(order))

        # Delete order that already completed.
        if order.status in [ORDER_STATUS_FAILED, ORDER_STATUS_CANCELED, ORDER_STATUS_FILLED]:
            self._orders.pop(order_no)

    async def on_event_asset_update(self, asset: Asset):
        """ Asset update callback.

        Args:
            asset: Asset object.
        """
        self._assets = asset
        SingleTask.run(self._asset_update_callback, asset)

from quant import const
from quant.utils.web import Websocket
from quant.order import ORDER_ACTION_BUY, ORDER_ACTION_SELL
from quant.event import EventTrade, EventKline, EventOrderbook

class BybitAccount:
    """ 
    Bybit,websocket账户信息推送
    
    """

    def __init__(self,ispublic_to_mq=False,islog=False, **kwargs):
        """Initialize Trade module.
        wss://stream.bybit.com/v5/private
        wss://stream.bybit.com/spot/private/v3
        """
        
        
        e = None
        if not kwargs.get("platform"):
            e = Error("param platform miss")
        if not kwargs.get("account"):
            e = Error("param account miss")
        if not kwargs.get("strategy"):
            e = Error("param strategy miss")
        if not kwargs.get("symbols"):
            e = Error("param symbols miss")
        if not kwargs.get("host"):
            kwargs["host"] = "https://api.bybit.com"
        if not kwargs.get("wss"):
            kwargs["wss"] = "wss://stream.bybit.com/spot/private/v3"
        if not kwargs.get("access_key"):
            e = Error("param access_key miss")
        if not kwargs.get("secret_key"):
            e = Error("param secret_key miss")
        if e:
            logger.error(e, caller=self)
            if kwargs.get("init_success_callback"):
                SingleTask.run(kwargs["init_success_callback"], False, e)
            return

        self.ispublic_to_mq=ispublic_to_mq
        self.islog=islog
        self._platform = kwargs.get("platform")
        self._account = kwargs["account"]
        self._strategy = kwargs["strategy"]
        self._platform = BYBIT
        self._symbols = kwargs["symbols"]
        self._host = kwargs["host"]
        self._wss = kwargs["wss"]
        self._access_key = kwargs["access_key"]
        self._secret_key = kwargs["secret_key"]
        self._asset_update_callback = kwargs.get("asset_update_callback")
        self._order_update_callback = kwargs.get("order_update_callback")

        url = self._wss
        self._ws = Websocket(url, connected_callback=self.send_auth,process_callback=self.process)
        self._ws.initialize()

        self._raw_symbols = []
        for symbol in self._symbols:
            self._raw_symbols.append(symbol.replace("_", ""))

        LoopRunTask.register(self.send_ping, 20)


        # Initialize our REST API client.
        #self._rest_api = BybitRestAPI(self._host, self._access_key, self._secret_key)


        # Create a loop run task to reset listen key every 30 minutes.
        #LoopRunTask.register(self._reset_listen_key, 60 * 30)

        # Create a coroutine to initialize Websocket connection.
        #SingleTask.run(self._init_websocket)

    #20秒發送一次心跳
    async def send_ping(self,*args, **kwargs):
        topic = {
            "op": "ping",
        }
        await self._ws.send(json.dumps(topic))

    async def send_auth(self):
        key = self._access_key
        secret = self._secret_key
        expires = int((time.time() + 10) * 1000)
        _val = f'GET/realtime{expires}'
        signature = str(hmac.new(
            bytes(secret, 'utf-8'),
            bytes(_val, 'utf-8'), digestmod='sha256'
        ).hexdigest())
        await self._ws.send(json.dumps({"op": "auth", "args": [key, expires, signature]}))

        topic = {
            "op": "subscribe",
            "args": [
                "order",
                "stopOrder",
                "outboundAccountInfo",
            ]
        }

        await self._ws.send(json.dumps(topic))
        logger.info("subscribe accountinf success.",topic,caller=self)


    @async_method_locker("BybitAccount.process")
    async def process(self, msg):
        """ Process message that received from Websocket connection.

        Args:
            msg: message received from Websocket connection.
        """
        try :
            e = msg.get("topic")
            if e == "order":  # Order update.
                data = msg.get("data")[0]
                if data:
                    pass
                else:
                    return
                if data["s"] not in self._raw_symbols:
                    return
                info = {
                    "platform": self._platform,
                    "account": self._account,
                    "strategy": self._strategy,
                    "Info":msg,
                }
                order = info
                if self._order_update_callback:
                    SingleTask.run(self._order_update_callback, copy.copy(order))
            if e == "stopOrder":  # Order update.
                data = msg.get("data")[0]
                if data:
                    pass
                else:
                    return
                if data["s"] not in self._raw_symbols:
                    return
                
                info = {
                    "platform": self._platform,
                    "account": self._account,
                    "strategy": self._strategy,
                    "Info":msg,
                }
                order = info
                if self._order_update_callback:
                    SingleTask.run(self._order_update_callback, copy.copy(order))
            if e == "outboundAccountInfo":  # asset update.
                """ [
                    {
                        "a": "USDT",
                        "f": "176.81254174",
                        "l": "201.575"
                    }
                ] """
                data = msg.get("data")[0]            
                if data:
                    pass
                else:
                    return
                assets = []
                for ba in data["B"]:
                    assets.append({
                                "a":ba.get("a"),
                                "f":float(ba.get("f")),
                                "l":float(ba.get("l"))
                            })
                info = {
                    "platform": self._platform,
                    "account": self._account,
                    "Info":msg,
                    "assets": assets,
                    "timestamp": int(data["E"]),
                    "update": int(msg["ts"])                    
                }
                asset = info
                if self._asset_update_callback:
                    SingleTask.run(self._asset_update_callback, copy.copy(asset))
        except Exception as e:
            pass
       

class BybitAccountV5:
    """ 
    Bybit,websocket账户信息推送
    
    """

    def __init__(self,ispublic_to_mq=False,islog=False, **kwargs):
        """Initialize Trade module.
        wss://stream.bybit.com/v5/private
        wss://stream.bybit.com/spot/private/v3
        """
        
        
        e = None
        if not kwargs.get("platform"):
            e = Error("param platform miss")
        if not kwargs.get("account"):
            e = Error("param account miss")
        if not kwargs.get("strategy"):
            e = Error("param strategy miss")
        if not kwargs.get("symbols"):
            e = Error("param symbols miss")
        if not kwargs.get("host"):
            kwargs["host"] = "https://api.bybit.com"
        if not kwargs.get("wss"):
            kwargs["wss"] = "wss://stream.bybit.com/v5/private"
        if not kwargs.get("access_key"):
            e = Error("param access_key miss")
        if not kwargs.get("secret_key"):
            e = Error("param secret_key miss")
        if e:
            logger.error(e, caller=self)
            if kwargs.get("init_success_callback"):
                SingleTask.run(kwargs["init_success_callback"], False, e)
            return

        self.ispublic_to_mq=ispublic_to_mq
        self.islog=islog
        self._platform = kwargs.get("platform")
        self._account = kwargs["account"]
        self._strategy = kwargs["strategy"]
        self._platform = BYBIT
        self._symbols = kwargs["symbols"]
        self._host = kwargs["host"]
        self._wss = kwargs["wss"]
        self._access_key = kwargs["access_key"]
        self._secret_key = kwargs["secret_key"]
        self._asset_update_callback = kwargs.get("asset_update_callback")
        self._order_update_callback = kwargs.get("order_update_callback")
        self._trader_update_callback = kwargs.get("trader_update_callback")


        url = self._wss
        self._ws = Websocket(url, connected_callback=self.send_auth,process_callback=self.process)
        self._ws.initialize()

        self._raw_symbols = []
        for symbol in self._symbols:
            self._raw_symbols.append(symbol.replace("_", ""))

        LoopRunTask.register(self.send_ping, 20)


        # Initialize our REST API client.
        #self._rest_api = BybitRestAPI(self._host, self._access_key, self._secret_key)


        # Create a loop run task to reset listen key every 30 minutes.
        #LoopRunTask.register(self._reset_listen_key, 60 * 30)

        # Create a coroutine to initialize Websocket connection.
        #SingleTask.run(self._init_websocket)

    #20秒發送一次心跳
    async def send_ping(self,*args, **kwargs):
        topic = {
            "op": "ping",
        }
        await self._ws.send(json.dumps(topic))

    async def send_auth(self):
        key = self._access_key
        secret = self._secret_key
        expires = int((time.time() + 10) * 1000)
        _val = f'GET/realtime{expires}'
        signature = str(hmac.new(
            bytes(secret, 'utf-8'),
            bytes(_val, 'utf-8'), digestmod='sha256'
        ).hexdigest())
        await self._ws.send(json.dumps({"op": "auth", "args": [key, expires, signature]}))

        topic = {
            "op": "subscribe",
            "args": [
                "order",
                "execution",
                "wallet",
            ]
        }

        await self._ws.send(json.dumps(topic))
        logger.info("subscribe accountinf success.",topic,caller=self)


    @async_method_locker("BybitAccount.process")
    async def process(self, msg):
        """ Process message that received from Websocket connection.

        Args:
            msg: message received from Websocket connection.
        """
        try :
            e = msg.get("topic")
            if e == "order":  # Order update.
                data = msg.get("data")[0]
                if data:
                    pass
                else:
                    return
                if data["symbol"] not in self._raw_symbols:
                    return
                info = {
                    "platform": self._platform,
                    "account": self._account,
                    "strategy": self._strategy,
                    "Info":msg,
                }
                order = info
                if self._order_update_callback:
                    SingleTask.run(self._order_update_callback, copy.copy(order))
            if e == "execution":  # Order update.
                data = msg.get("data")[0]
                if data:
                    pass
                else:
                    return
                if data["symbol"] not in self._raw_symbols:
                    return
                
                info = {
                    "platform": self._platform,
                    "account": self._account,
                    "strategy": self._strategy,
                    "Info":msg,
                }
                order = info
                if self._trader_update_callback:
                    SingleTask.run(self._trader_update_callback, copy.copy(order))
            if e == "wallet":  # asset update.
                """ [
                    {
                        "a": "USDT",
                        "f": "176.81254174",
                        "l": "201.575"
                    }
                ] """
                data = msg.get("data")[0]            
                if data:
                    pass
                else:
                    return
                assets = []
                for ba in data["coin"]:
                    assets.append({
                                "a":ba.get("coin"),
                                "f":float(ba.get("availableToWithdraw")),
                                "l":float(ba.get("walletBalance")) - float(ba.get("availableToWithdraw"))
                            })
                info = {
                    "platform": self._platform,
                    "account": self._account,
                    "Info":msg,
                    "assets": assets,
                    "timestamp": int(msg["creationTime"]),
                    "update":int(msg["creationTime"])                
                }
                asset = info
                if self._asset_update_callback:
                    SingleTask.run(self._asset_update_callback, copy.copy(asset))
        except Exception as e:
            pass


#交易所行情数据对象
#通过ws接口获取行情，并通过回调函数传递给策略使用
class BybitMarketV3:
    """ Bybit Market Server.

    Attributes:
        ispublic_to_mq:是否将行情推送到行情中心,默认为否
        islog:是否logger输出行情数据,默认为否
        orderbook_update_callback:订单薄数据回调函数
        kline_update_callback:K线数据回调函数
        trade_update_callback:成交数据回调函数
        kwargs:
            platform: Exchange platform name, must be `Bybit`.
            wss: Exchange Websocket host address, default is `wss://stream.bybit.com/spot/public/v3`.
            symbols: Symbol list.
            channels: Channel list, only `orderbook` / `trade` / `kline` to be enabled.
            orderbook_length: The length of orderbook's data to be published via OrderbookEvent, default is 10.
    """

    def __init__(self,ispublic_to_mq=False,islog=False, orderbook_update_callback=None,kline_update_callback=None,trade_update_callback=None,tickers_update_callback=None, **kwargs):
        self.ispublic_to_mq=ispublic_to_mq
        self.islog=islog
        self._platform = kwargs.get("platform")
        self._account = kwargs.get("account")

        self._wss = kwargs.get("wss", "wss://stream.bybit.com/spot/public/v3")
        self._symbols = list(set(kwargs.get("symbols")))
        self._channels = kwargs.get("channels")
        self._orderbook_length = kwargs.get("orderbook_length", 40)

        self._c_to_s = {}
        self._tickers = {}
        self.isalive = False


        url = self._make_url()
        self._ws = Websocket(url, connected_callback=self.connected_callback,process_callback=self.BybitMarket_process)
        self._ws.initialize()
        LoopRunTask.register(self.send_heartbeat_msg, 20)
        
       
        kwargs["orderbook_update_callback"] = self.process_orderbook
        kwargs["kline_update_callback"] = self.process_kline
        kwargs["trade_update_callback"] = self.process_trade
        kwargs["tickers_update_callback"] = self.process_tickers
        self._orderbook_update_callback = orderbook_update_callback
        self._kline_update_callback = kline_update_callback
        self._trade_update_callback = trade_update_callback
        self._tickers_update_callback = tickers_update_callback
    
    async def send_heartbeat_msg(self, *args, **kwargs):
        d = {
            "op": "ping",
        }
        if not self._ws:
            self.isalive = False
            logger.error("Websocket connection not yeah!", caller=self)
            return
        await self._ws.send(json.dumps(d))

    async def connected_callback(self,*fargs, **kwargs):
        if not self._symbols:
            logger.warn("symbols not found in config file.", caller=self)
            return
        if not self._channels:
            logger.warn("channels not found in config file.", caller=self)
            return
        topicss = []
        for ch in self._channels:
            if ch == "kline":
                topics=[]
                args = []
                for symbol in self._symbols: 
                    symbol=symbol.replace("_","")
                    args.append("kline.1m."+symbol)
                culist = tools.cut_list(args,10)
                for arg in culist:
                    if len(arg)<=10:
                        topics.append({
                                "op": "subscribe",
                                "args": arg
                                })
                topicss.append(topics)                   
            elif ch == "orderbook":
                topics=[]
                args = []
                for symbol in self._symbols:
                    symbol=symbol.replace("_","")
                    args.append("orderbook."+str(self._orderbook_length)+"."+symbol)
                culist = tools.cut_list(args,10)
                for arg in culist:
                    if len(arg)<=10:
                        topics.append({
                                "op": "subscribe",
                                "args": arg
                                })
                topicss.append(topics)                  
            elif ch == "trade":
                topics=[]
                args = []
                for symbol in self._symbols:
                    symbol=symbol.replace("_","")
                    args.append("trade."+symbol)
                culist = tools.cut_list(args,10)
                for arg in culist:
                    if len(arg)<=10:
                        topics.append({
                                "op": "subscribe",
                                "args": arg
                                })
                topicss.append(topics) 
            elif ch == "tickers":
                topics=[]
                args = []
                for symbol in self._symbols:
                    symbol=symbol.replace("_","")
                    args.append("bookticker."+symbol)
                culist = tools.cut_list(args,10)
                for arg in culist:
                    if len(arg)<=10:
                        topics.append({
                                "op": "subscribe",
                                "args": arg
                                })
                topicss.append(topics) 
            else:
                logger.error("channel error! channel:", ch, caller=self)
        for tos in topicss:
            for to in tos:
                await self._ws.send(json.dumps(to))
                logger.info("subscribe",to,"success.", caller=self)
        return

    def _make_url(self):
        """Generate request url.
        """
        url = self._wss
        return url

    async def BybitMarket_process(self, msg):
        """Process message that received from Websocket connection.

        Args:
            msg: Message received from Websocket connection.
        """
        #logger.debug("msg:", msg, caller=self)
        self.isalive = True
        if not isinstance(msg, dict):
            return

        channel = msg.get("topic","").split(".")[0]
        #if channel not in self._channels:
        #    logger.warn("unkown channel, msg:", msg, caller=self)
        #    return

        data = msg.get("data")
        symbol = data.get("s")
        if data:
            pass
        else:
            #logger.info("返回数据有误:", msg, caller=self)
            return
        if "kline" in channel:
            await self.process_kline(symbol, data,msg)
        elif "orderbook" in channel:
            await self.process_orderbook(symbol, data,msg)
        elif "trade" in channel:
            symbol = msg.get("topic").split(".")[1]
            await self.process_trade(symbol, data,msg)
        elif "bookticker" in channel:
            await self.process_tickers(symbol, data,msg)

    #加修饰器使得行情信息依次处理,如果之前的数据未处理完新数据直接抛弃，避免数据堆积新旧穿插
    #@async_method_locker("BybitMarketv3_process_kline",wait=False)
    async def process_kline(self, symbol, data,msg):
        """Process kline data and publish KlineEvent."""
        kline = {
            "Info":msg,
            "platform": self._platform,
            "symbol": symbol,
            "Open": float(data.get("o")),
            "High": float(data.get("h")),
            "Low": float(data.get("l")),
            "Close": float(data.get("c")),
            "Volume": float(data.get("v")),
            "Time": msg.get("ts"),
        }
        if self.ispublic_to_mq:
            EventKline(**kline).publish()
        if self._kline_update_callback:
            SingleTask.run(self._kline_update_callback, kline)
        if self.islog:
            logger.info("symbol:", symbol, "kline:", kline, caller=self)

    #加修饰器使得行情信息依次处理,如果之前的数据未处理完新数据直接抛弃，避免数据堆积新旧穿插
    #@async_method_locker("BybitMarketv3_process_orderbook",wait=True)
    async def process_orderbook(self, symbol, data,msg):
        """Process orderbook data and publish OrderbookEvent."""
        # bids = []
        # asks = []
        # for bid in data.get("b")[:self._orderbook_length]:
        #     bids.append({"Price":float(bid[0]),"Amount":float(bid[1])})
        # for ask in data.get("a")[:self._orderbook_length]:
        #     asks.append({"Price":float(ask[0]),"Amount":float(ask[1])})
        orderbook = {
            "Info":msg,
            "platform": self._platform,
            "symbol": symbol,
            "Asks": data.get("a")[:self._orderbook_length],
            "Bids": data.get("b")[:self._orderbook_length],
            "Time": msg.get("ts")
        }
        if self._orderbook_update_callback:
            SingleTask.run(self._orderbook_update_callback, orderbook)
        if self.ispublic_to_mq:
            EventOrderbook(**orderbook).publish()
        if self.islog:
            logger.info("symbol:", symbol, "orderbook:", orderbook, caller=self)

    #加修饰器使得行情信息依次处理,如果之前的数据未处理完新数据直接抛弃，避免数据堆积新旧穿插
    #@async_method_locker("BybitMarketv3_process_trade",wait=False)
    async def process_trade(self, symbol, data,msg):
        """Process trade data and publish TradeEvent."""
        trade = {
            "Info":msg,
            "platform": self._platform,
            "symbol": symbol,
            "Type":  0 if data["m"] else 1, 
            "Price": data.get("p"),
            "Amount": data.get("q"),
            "Time": msg.get("ts")
        }
        if self.ispublic_to_mq:
            EventTrade(**trade).publish()
        if self._trade_update_callback:
            SingleTask.run(self._trade_update_callback, trade)
        if self.islog:
            logger.info("symbol:", symbol, "trade:", trade, caller=self)
    
    #加修饰器使得行情信息依次处理,如果之前的数据未处理完新数据直接抛弃，避免数据堆积新旧穿插
    #@async_method_locker("BybitMarketv3_process_tickers",wait=False)
    async def process_tickers(self, symbol, data,msg):
        """Process aggTrade data and publish TradeEvent."""
        ticker = {
            "Info":msg,
            "platform": self._platform,
            "symbol": symbol,
            "Sellamount"     : data.get("aq"),              
            "Sell"    : data.get("ap"),              
            "Buy"     : data.get("bp"),              
            "Buyamount"    : data.get("bq"),             
            "Time"    : msg.get("ts")   
        }
        if self.ispublic_to_mq:
            EventTrade(**ticker).publish()
        if self._tickers_update_callback:
            SingleTask.run(self._tickers_update_callback, ticker)
        if self.islog:
            logger.info("symbol:", symbol, "ticker:", ticker, caller=self)

#暂时未支持
class BybitMarketV5:
    """ Bybit Market Server.

    Attributes:
        ispublic_to_mq:是否将行情推送到行情中心,默认为否
        islog:是否logger输出行情数据,默认为否
        orderbook_update_callback:订单薄数据回调函数
        kline_update_callback:K线数据回调函数
        trade_update_callback:成交数据回调函数
        kwargs:
            platform: Exchange platform name, must be `Bybit`.
            wss: Exchange Websocket host address, default is `wss://stream.bybit.com/spot/public/v3`.
            symbols: Symbol list.
            channels: Channel list, only `orderbook` / `trade` / `kline` to be enabled.
            orderbook_length: The length of orderbook's data to be published via OrderbookEvent, default is 10.
    """

    def __init__(self,ispublic_to_mq=False,islog=False, orderbook_update_callback=None,kline_update_callback=None,trade_update_callback=None,tickers_update_callback=None, **kwargs):
        self.ispublic_to_mq=ispublic_to_mq
        self.islog=islog
        self._platform = kwargs.get("platform")
        self._account = kwargs.get("account")

        self._wss = kwargs.get("wss", "wss://stream.bybit.com/v5/public/spot")
        self._symbols = list(set(kwargs.get("symbols")))
        self._channels = kwargs.get("channels")
        self._orderbook_length = kwargs.get("orderbook_length", 50)

        self._c_to_s = {}
        self._tickers = {}
        self._orderbooks = {}
        self._bookone = {}
        self.isalive = False


        url = self._make_url()
        self._ws = Websocket(url, connected_callback=self.connected_callback,process_callback=self.BybitMarket_process)
        self._ws.initialize()
        LoopRunTask.register(self.send_heartbeat_msg, 20)

        
       
        kwargs["orderbook_update_callback"] = self.process_orderbook
        kwargs["kline_update_callback"] = self.process_kline
        kwargs["trade_update_callback"] = self.process_trade
        kwargs["tickers_update_callback"] = self.process_tickers
        self._orderbook_update_callback = orderbook_update_callback
        self._kline_update_callback = kline_update_callback
        self._trade_update_callback = trade_update_callback
        self._tickers_update_callback = tickers_update_callback
    
    async def send_heartbeat_msg(self, *args, **kwargs):
        d = {
            "op": "ping",
        }
        if not self._ws:
            self.isalive = False
            logger.error("Websocket connection not yeah!", caller=self)
            return
        await self._ws.send(json.dumps(d))
    

    async def connected_callback(self,*fargs, **kwargs):
        if not self._symbols:
            logger.warn("symbols not found in config file.", caller=self)
            return
        if not self._channels:
            logger.warn("channels not found in config file.", caller=self)
            return
        topicss = []
        for ch in self._channels:
            if ch == "kline":
                topics=[]
                args = []
                for symbol in self._symbols: 
                    symbol=symbol.replace("_","")
                    args.append("kline.1."+symbol)
                culist = tools.cut_list(args,10)
                for arg in culist:
                    if len(arg)<=10:
                        topics.append({
                                "op": "subscribe",
                                "args": arg
                                })
                topicss.append(topics)                   
            elif ch == "orderbook":
                topics=[]
                args = []
                books = 50
                if self._orderbook_length==1:
                    books = 1
                for symbol in self._symbols:
                    symbol=symbol.replace("_","")
                    self._orderbooks[symbol]={}
                    args.append("orderbook."+str(books)+"."+symbol)
                culist = tools.cut_list(args,10)
                for arg in culist:
                    if len(arg)<=10:
                        topics.append({
                                "op": "subscribe",
                                "args": arg
                                })
                topicss.append(topics)                  
            elif ch == "bookone":
                topics=[]
                args = []
                for symbol in self._symbols:
                    symbol=symbol.replace("_","")
                    self._bookone[symbol]={}
                    args.append("orderbook."+str(1)+"."+symbol)
                culist = tools.cut_list(args,10)
                for arg in culist:
                    if len(arg)<=10:
                        topics.append({
                                "op": "subscribe",
                                "args": arg
                                })
                topicss.append(topics)                  
            elif ch == "trade":
                topics=[]
                args = []
                for symbol in self._symbols:
                    symbol=symbol.replace("_","")
                    args.append("publicTrade."+symbol)
                culist = tools.cut_list(args,10)
                for arg in culist:
                    if len(arg)<=10:
                        topics.append({
                                "op": "subscribe",
                                "args": arg
                                })
                topicss.append(topics) 
            elif ch == "tickers":
                topics=[]
                args = []
                for symbol in self._symbols:
                    symbol=symbol.replace("_","")
                    args.append("tickers."+symbol)
                culist = tools.cut_list(args,10)
                for arg in culist:
                    if len(arg)<=10:
                        topics.append({
                                "op": "subscribe",
                                "args": arg
                                })
                topicss.append(topics) 
            else:
                logger.error("channel error! channel:", ch, caller=self)
        for tos in topicss:
            for to in tos:
                await self._ws.send(json.dumps(to))
                logger.info("subscribe",to,"success.", caller=self)
        return

    def _make_url(self):
        """Generate request url.
        """
        url = self._wss
        return url

    async def BybitMarket_process(self, msg):
        """Process message that received from Websocket connection.

        Args:
            msg: Message received from Websocket connection.
        """
        #logger.debug("msg:", msg, caller=self)
        self.isalive = True
        if not isinstance(msg, dict):
            return

        channel = msg.get("topic","")
        #if channel not in self._channels:
        #    logger.warn("unkown channel, msg:", msg, caller=self)
        #    return

        data = msg.get("data")
        if data:
            pass
        else:
            #logger.info("返回数据有误:", msg, caller=self)
            return
        if "kline" in channel:
            symbol = msg.get("topic").split(".")[2]
            await self.process_kline(symbol, data,msg)
        elif "orderbook" in channel:
            symbol = data.get("s")
            otype = msg.get("type")
            if "orderbook.1" in channel:
                if otype=="snapshot":
                    await self.process_bookone_partial(symbol,msg)
                if otype=="delta":
                    await self.deal_bookone_update(symbol,msg)
            else:
                if otype=="snapshot":
                    await self.process_orderbook_partial(symbol,msg)
                if otype=="delta":
                    await self.deal_orderbook_update(symbol,msg)

        elif "publicTrade" in channel:
            symbol = data[0].get("s")
            await self.process_trade(symbol, data,msg)
        elif "tickers" in channel:
            symbol = data.get("symbol")
            await self.process_tickers(symbol, data,msg)
    
    async def process_orderbook_partial(self,symbol, msg):
        """
        全量订单薄数据
        Process orderbook partical data.
        """
        if symbol not in self._orderbooks:
            return
        asks = msg["data"].get("a")
        bids = msg["data"].get("b")
        self._orderbooks[symbol] = {
                "Asks": {}, 
                "Bids": {}, 
                "Time": 0
            }
        for ask in asks:
            price = ask[0]
            quantity = ask[1]
            self._orderbooks[symbol]["Asks"][price] = quantity
        for bid in bids:
            price = bid[0]
            quantity = bid[1]
            self._orderbooks[symbol]["Bids"][price] = quantity
        self._orderbooks[symbol]["Time"] = msg["data"].get("ts")

    @async_method_locker("BybitMarketV5_deal_orderbook_update",wait=True)
    async def deal_orderbook_update(self, symbol, msg):
        """
        增量订单薄数据
        Process orderbook update data.
        """
        asks = msg["data"].get("a")
        bids = msg["data"].get("b")
        timestamp = msg.get("ts")

        if symbol not in self._orderbooks:
            return
        self._orderbooks[symbol]["Time"] = timestamp

        for ask in asks:
            price = ask[0]
            quantity = ask[1]
            if float(quantity) == 0 and price in self._orderbooks[symbol]["Asks"]:
                self._orderbooks[symbol]["Asks"].pop(price)
            else:
                self._orderbooks[symbol]["Asks"][price] = quantity

        for bid in bids:
            price = bid[0]
            quantity = bid[1]
            if float(quantity) == 0 and price in self._orderbooks[symbol]["Bids"]:
                self._orderbooks[symbol]["Bids"].pop(price)
            else:
                self._orderbooks[symbol]["Bids"][price] = quantity

        await self.process_orderbook(symbol,msg)
    
    async def process_bookone_partial(self,symbol, msg):
        """
        全量订单薄数据
        Process orderbook partical data.
        """
        if symbol not in self._bookone:
            return
        asks = msg["data"].get("a")
        bids = msg["data"].get("b")
        self._bookone[symbol] = {
                "Asks": {}, 
                "Bids": {}, 
                "Time": 0
            }
        for ask in asks:
            price = ask[0]
            quantity = ask[1]
            self._bookone[symbol]["Asks"][price] = quantity
        for bid in bids:
            price = bid[0]
            quantity = bid[1]
            self._bookone[symbol]["Bids"][price] = quantity
        self._bookone[symbol]["Time"] = msg["data"].get("ts")

    @async_method_locker("BybitMarketV5_deal_orderbook_update",wait=True)
    async def deal_bookone_update(self, symbol, msg):
        """
        增量订单薄数据
        Process orderbook update data.
        """
        asks = msg["data"].get("a")
        bids = msg["data"].get("b")
        timestamp = msg.get("ts")

        if symbol not in self._bookone:
            return
        self._bookone[symbol]["Time"] = timestamp

        for ask in asks:
            price = ask[0]
            quantity = ask[1]
            if float(quantity) == 0 and price in self._bookone[symbol]["Asks"]:
                self._bookone[symbol]["Asks"].pop(price)
            else:
                self._bookone[symbol]["Asks"][price] = quantity

        for bid in bids:
            price = bid[0]
            quantity = bid[1]
            if float(quantity) == 0 and price in self._bookone[symbol]["Bids"]:
                self._bookone[symbol]["Bids"].pop(price)
            else:
                self._bookone[symbol]["Bids"][price] = quantity

        await self.process_bookone(symbol,msg)
    
    def check(self,bids, asks):
        # 获取bid档str
        bids_l = []
        bid_l = []
        count_bid = 1
        while count_bid <= 25:
            if count_bid > len(bids):
                break
            bids_l.append(bids[count_bid-1])
            count_bid += 1
        for j in bids_l:
            str_bid = ':'.join(j[0 : 2])
            bid_l.append(str_bid)
        # 获取ask档str
        asks_l = []
        ask_l = []
        count_ask = 1
        while count_ask <= 25:
            if count_ask > len(asks):
                break
            asks_l.append(asks[count_ask-1])
            count_ask += 1
        for k in asks_l:
            str_ask = ':'.join(k[0 : 2])
            ask_l.append(str_ask)
        # 拼接str
        num = ''
        if len(bid_l) == len(ask_l):
            for m in range(len(bid_l)):
                num += bid_l[m] + ':' + ask_l[m] + ':'
        elif len(bid_l) > len(ask_l):
            # bid档比ask档多
            for n in range(len(ask_l)):
                num += bid_l[n] + ':' + ask_l[n] + ':'
            for l in range(len(ask_l), len(bid_l)):
                num += bid_l[l] + ':'
        elif len(bid_l) < len(ask_l):
            # ask档比bid档多
            for n in range(len(bid_l)):
                num += bid_l[n] + ':' + ask_l[n] + ':'
            for l in range(len(bid_l), len(ask_l)):
                num += ask_l[l] + ':'

        new_num = num[:-1]
        int_checksum = zlib.crc32(new_num.encode())
        fina = self.change(int_checksum)
        return fina
    def change(self,num_old):
        num = pow(2, 31) - 1
        if num_old > num:
            out = num_old - num * 2 - 2
        else:
            out = num_old
        return out

    #@async_method_locker("BybitMarketV5_process_orderbook",wait=True)
    async def process_orderbook(self, symbol,msg,*args, **kwargs):
        """Publish OrderbookEvent."""
        ob = copy.copy(self._orderbooks[symbol])
        if not ob["Asks"] or not ob["Bids"]:
            logger.warn("symbol:", symbol, "Asks:", ob["Asks"], "Bids:", ob["Bids"], caller=self)
            return

        ask_keys = sorted(list(ob["Asks"].keys()))
        bid_keys = sorted(list(ob["Bids"].keys()), reverse=True)
        if float(ask_keys[0]) <= float(bid_keys[0]):
            logger.warn("bybit深度信息合并有误,尝试重启wss", symbol, "ask1:", ask_keys[0], "bid1:", bid_keys[0], caller=self)
            return
        # 检查合并是否正确
        # asks = []
        # for k in ask_keys[:self._orderbook_length]:
        #     price = k
        #     quantity = ob["Asks"].get(k)
        #     asks.append([price, quantity])

        # bids = []
        # for k in bid_keys[:self._orderbook_length]:
        #     price = k
        #     quantity = ob["Bids"].get(k)
        #     bids.append([price, quantity])
        # checksum=msg.get("data")[0].get("checksum")
        # checksum0=self.check(bids,asks)
        # print(checksum0,checksum)

        asks = []
        for k in ask_keys[:self._orderbook_length]:
            price = k
            quantity = ob["Asks"].get(k)
            asks.append({"Price":price, "Amount":quantity})

        bids = []
        for k in bid_keys[:self._orderbook_length]:
            price = k
            quantity = ob["Bids"].get(k)
            bids.append({"Price":price, "Amount":quantity})

        orderbook = {
            "Info":msg,
            "platform": self._platform,
            "symbol": symbol,
            "Asks": asks,
            "Bids": bids,
            "Time": ob["Time"]
        }
        if self._orderbook_update_callback:
            SingleTask.run(self._orderbook_update_callback, orderbook)
        if self.ispublic_to_mq:
            EventOrderbook(**orderbook).publish()
        if self.islog:
            logger.debug("symbol:", symbol, "orderbook:", orderbook, caller=self)

    #加修饰器使得行情信息依次处理,如果之前的数据未处理完新数据直接抛弃，避免数据堆积新旧穿插
    #@async_method_locker("BybitMarketv3_process_kline",wait=False)
    async def process_kline(self, symbol, data,msg):
        """Process kline data and publish KlineEvent."""
        kline = {
            "Info":msg,
            "platform": self._platform,
            "symbol": symbol,
            "Open": float(data[0].get("open")),
            "High": float(data[0].get("high")),
            "Low": float(data[0].get("low")),
            "Close": float(data[0].get("close")),
            "Volume": float(data[0].get("volume")),
            "Time": data[0].get("timestamp"),
        }
        if self.ispublic_to_mq:
            EventKline(**kline).publish()
        if self._kline_update_callback:
            SingleTask.run(self._kline_update_callback, kline)
        if self.islog:
            logger.info("symbol:", symbol, "kline:", kline, caller=self)


    #加修饰器使得行情信息依次处理,如果之前的数据未处理完新数据直接抛弃，避免数据堆积新旧穿插
    #@async_method_locker("BybitMarketv3_process_trade",wait=False)
    async def process_trade(self, symbol, data,msg):
        """Process trade data and publish TradeEvent."""
        trade = {
            "Info":msg,
            "platform": self._platform,
            "symbol": symbol,
            "Type":  0 if data[0]["S"]=="Buy" else 1, 
            "Price": data[0].get("p"),
            "Amount": data[0].get("v"),
            "Time": msg.get("ts")
        }
        if self.ispublic_to_mq:
            EventTrade(**trade).publish()
        if self._trade_update_callback:
            SingleTask.run(self._trade_update_callback, trade)
        if self.islog:
            logger.info("symbol:", symbol, "trade:", trade, caller=self)
    
    #加修饰器使得行情信息依次处理,如果之前的数据未处理完新数据直接抛弃，避免数据堆积新旧穿插
    #@async_method_locker("BybitMarketv3_process_tickers",wait=False)
    async def process_tickers(self, symbol, data,msg):
        """Process aggTrade data and publish TradeEvent."""
        ticker = {
            "Info":msg,
            "platform": self._platform,
            "symbol": symbol,
            "Sellamount"     : data.get("volume24h"),              
            "Sell"    : data.get("lastPrice"),              
            "Buy"     : data.get("lastPrice"),              
            "Buyamount"    : data.get("volume24h"),             
            "Time"    : msg.get("ts")   
        }
        if self.ispublic_to_mq:
            EventTrade(**ticker).publish()
        if self._tickers_update_callback:
            SingleTask.run(self._tickers_update_callback, ticker)
        if self.islog:
            logger.info("symbol:", symbol, "ticker:", ticker, caller=self)
    
    #加修饰器使得行情信息依次处理,如果之前的数据未处理完新数据直接抛弃，避免数据堆积新旧穿插
    #@async_method_locker("BybitMarketv3_process_bookone",wait=False)
    async def process_bookone(self, symbol, msg):
        """Process aggTrade data and publish TradeEvent."""
        ob = copy.copy(self._bookone[symbol])
        if not ob["Asks"] or not ob["Bids"]:
            logger.warn("symbol:", symbol, "Asks:", ob["Asks"], "Bids:", ob["Bids"], caller=self)
            return

        ask_keys = sorted(list(ob["Asks"].keys()))
        bid_keys = sorted(list(ob["Bids"].keys()), reverse=True)
        if float(ask_keys[0]) <= float(bid_keys[0]):
            logger.warn("bybit深度信息合并有误,尝试重启wss", symbol, "ask1:", ask_keys[0], "bid1:", bid_keys[0], caller=self)
            return
        asks = []
        for k in ask_keys[:self._orderbook_length]:
            price = k
            quantity = ob["Asks"].get(k)
            asks.append({"Price":price, "Amount":quantity})
        bids = []
        for k in bid_keys[:self._orderbook_length]:
            price = k
            quantity = ob["Bids"].get(k)
            bids.append({"Price":price, "Amount":quantity})
        ticker = {
            "Info":msg,
            "platform": self._platform,
            "symbol": symbol,
            "Sellamount"     : asks[0]["Amount"],              
            "Sell"    : asks[0]["Price"],              
            "Buy"     : bids[0]["Price"],              
            "Buyamount"    : bids[0]["Amount"],             
            "Time"    : ob["Time"]   
        }
        if self._tickers_update_callback:
            SingleTask.run(self._tickers_update_callback, ticker)
        if self.ispublic_to_mq:
            EventTrade(**ticker).publish()
        if self.islog:
            logger.info("symbol:", symbol, "ticker:", ticker, caller=self)