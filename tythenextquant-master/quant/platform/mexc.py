# -*- coding:utf-8 -*-

"""
Mexc.io Trade module.
https://Mexcio.news/api2#spot

Author: xunfeng
Date:   2019/07/15
Email:  xunfeng@test.com
"""

import copy
import hmac
import hashlib
import urllib
from urllib.parse import urljoin
import math
import traceback

from quant.error import Error
from quant.utils import logger
from quant.const import MEXC
from quant.order import Order
from quant.asset import Asset, AssetSubscribe
from quant.tasks import SingleTask, LoopRunTask
from quant.utils.http_client import AsyncHttpRequests
from quant.utils.http_client import SyncHttpRequests
import time
import json
from quant.utils import tools
from quant.utils.decorator import async_method_locker
from quant.order import ORDER_TYPE_LIMIT, ORDER_ACTION_BUY, ORDER_ACTION_SELL
from quant.order import ORDER_STATUS_SUBMITTED, ORDER_STATUS_PARTIAL_FILLED, ORDER_STATUS_FILLED, \
    ORDER_STATUS_CANCELED, ORDER_STATUS_FAILED


__all__ = ("MexcRestAPI", "MexcTrade", )


class MexcRestAPI:
    """ Mexc REST API client.
    https://api.mexc.com

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
                exchangeInfo, error = await self.request("GET", "/api/v3/exchangeInfo", "")
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
                    for info in exchangeInfo["symbols"]:
                        if info["symbol"]==exsymbol:
                            minQty=float(info["baseSizePrecision"])
                            maxQty=float(info["maxQuoteAmount"])
                            amountSize = int(info["baseAssetPrecision"])
                            priceSize=int(info["quotePrecision"])
                            tickSize = float(10**(-1*int(info["quotePrecision"])))
                            minNotional = float(info["quoteAmountPrecision"]) #名义价值
                            success={
                                "Info"    : exchangeInfo if reinfo else "",             #请求交易所接口后，交易所接口应答的原始数据
                                "symbol":symbol,
                                "minQty"    : minQty,              #最小下单量
                                "maxQty"     : maxQty,               #最大下单量
                                "amountSize": amountSize,                    #数量精度位数
                                "priceSize"    : priceSize,               #价格精度位数
                                "tickSize"     : tickSize,               #单挑价格
                                "minNotional"    :minNotional,               #最小订单名义价值
                                "Time"    : successres["serverTime"]      #毫秒级别时间戳
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
                success, error = await self.request("GET", "/api/v3/ticker/24hr", params)
                successres = success
                if success:
                    success={
                        "Info"    : success,             #请求交易所接口后，交易所接口应答的原始数据，回测时无此属性
                        "High"    : float(success["highPrice"]),              #最高价，如果交易所接口没有提供24小时最高价则使用卖一价格填充
                        "Low"     : float(success["lowPrice"]),               #最低价，如果交易所接口没有提供24小时最低价则使用买一价格填充
                        "Sellamount": float(success["askQty"]),
                        "Sell"    : float(success["askPrice"]),               #卖一价
                        "Buy"     : float(success["bidPrice"]),               #买一价
                        "Buyamount": float(success["bidQty"]),
                        "Last"    : float(success["lastPrice"]),               #最后成交价
                        "Volume"  : float(success["volume"]),          #最近成交量
                        "Time"    : success["closeTime"]      #毫秒级别时间戳
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
                success, error = await self.request("GET", "/api/v3/depth", params)
                successres = success
                if success:
                    Bids=[]
                    Asks=[]
                    for dp in success["bids"][0:limit]:
                        Bids.append({"Price":float(dp[0]),"Amount":float(dp[1])})
                    for dp in success["asks"][0:limit]:
                        Asks.append({"Price":float(dp[0]),"Amount":float(dp[1])})
                    success={
                        "Info"    : success,
                        "Asks"    : Asks,             #卖单数组，MarketOrder数组,按价格从低向高排序
                        "Bids"    : Bids,             #买单数组，MarketOrder数组,按价格从高向低排序
                        "Time"    : ts      #毫秒级别时间戳
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
                if not autotry:
                    logger.info("depth行情更新报错",successres,error,e,caller=self)
                    break
                else:
                    logger.info("depth行情更新报错，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                    await tools.Sleep(sleep/1000)
        return success, error
    
    async def GetTrades(self,symbol=None,limit=100,autotry = False,sleep=100):
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
                success, error = await self.request("GET", "/api/v3/aggTrades", params)
                successres = success
                if success:
                    aggTrades = []
                    for tr in success:
                        aggTrades.append({
                            "Id":tr["a"],
                            "Time":tr["T"],
                            "Price":float(tr["p"]),
                            "Amount":float(tr["q"]),
                            "Type":1 if tr["m"] else 0,
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

    async def GetAccount(self,basesymbol=None,quotesymbol=None,autotry = False,sleep=100):
        """ Get user account information.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        ts = tools.get_cur_timestamp_ms()
        params = {
            "timestamp": str(ts)
        }
        successres=None
        if basesymbol and quotesymbol:
            while True:
                try:
                    success, error = await self.request("GET", "/api/v3/account", params, auth=True)
                    successres = success
                    if success:
                        Stocks = 0
                        FrozenStocks = 0
                        Balance = 0
                        FrozenBalance = 0
                        for ac in success["balances"]:
                            if ac["asset"]==basesymbol:
                                Stocks=float(ac["free"])
                                FrozenStocks=float(ac["locked"])
                            if ac["asset"]==quotesymbol:
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
                        success=[]
                    if not autotry:
                        break
                    else:
                        logger.info("账户信息更新失败，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                        await tools.Sleep(sleep/1000)
                except Exception as e:
                    success = []
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
        if ttype == "LIMIT" and timeInForce=="IOC":
            info = {
                "symbol": symbol,
                "side": "BUY",
                "type": "IMMEDIATE_OR_CANCEL",
                "quantity": quantity,
                "price": price,
                "recvWindow": "5000",
                "timestamp": tools.get_cur_timestamp_ms(),
            }
        if ttype == "LIMIT" and timeInForce=="GTC":
            info = {
                "symbol": symbol,
                "side": "BUY",
                "type": "LIMIT",
                "quantity": quantity,
                "price": price,
                "recvWindow": "5000",
                "timestamp": tools.get_cur_timestamp_ms(),
            }
        
        if ttype == "LIMIT" and timeInForce=="FOK":
            info = {
                "symbol": symbol,
                "side": "BUY",
                "type": "FILL_OR_KILL",
                "quantity": quantity,
                "price": price,
                "recvWindow": "5000",
                "timestamp": tools.get_cur_timestamp_ms(),
            }
        if ttype=="LIMIT_MAKER":
            info = {
                "symbol": symbol,
                "side": "BUY",
                "type": "LIMIT_MAKER",
                "quantity": quantity,
                "price": price,
                "recvWindow": "5000",
                "timestamp": tools.get_cur_timestamp_ms()
            }
        if ttype=="MARKET":
            info = {
                "symbol": symbol,
                "side": "BUY",
                "type": "MARKET",
                "quantity": quantity,
                "recvWindow": "5000",
                "timestamp": tools.get_cur_timestamp_ms()
            }
        
        success, error = await self.request("POST", "/api/v3/order", body=info, auth=True,logrequestinfo=logrequestinfo)
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
        if ttype == "LIMIT" and timeInForce=="IOC":
            info = {
                "symbol": symbol,
                "side": "SELL",
                "type": "IMMEDIATE_OR_CANCEL",
                "quantity": quantity,
                "price": price,
                "recvWindow": "5000",
                "timestamp": tools.get_cur_timestamp_ms(),
            }
        if ttype == "LIMIT" and timeInForce=="GTC":
            info = {
                "symbol": symbol,
                "side": "SELL",
                "type": "LIMIT",
                "quantity": quantity,
                "price": price,
                "recvWindow": "5000",
                "timestamp": tools.get_cur_timestamp_ms(),
            }        
        if ttype == "LIMIT" and timeInForce=="FOK":
            info = {
                "symbol": symbol,
                "side": "SELL",
                "type": "FILL_OR_KILL",
                "quantity": quantity,
                "price": price,
                "recvWindow": "5000",
                "timestamp": tools.get_cur_timestamp_ms(),
            }
        if ttype=="LIMIT_MAKER":
            info = {
                "symbol": symbol,
                "side": "SELL",
                "type": "LIMIT_MAKER",
                "quantity": quantity,
                "price": price,
                "recvWindow": "5000",
                "timestamp": tools.get_cur_timestamp_ms()
            }
        if ttype=="MARKET":
            info = {
                "symbol": symbol,
                "side": "SELL",
                "type": "MARKET",
                "quantity": quantity,
                "recvWindow": "5000",
                "timestamp": tools.get_cur_timestamp_ms()
            }
        success, error = await self.request("POST", "/api/v3/order", body=info, auth=True,logrequestinfo=logrequestinfo)
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
                "symbol": symbol,
                "orderId": str(order_id),
                "origClientOrderId": client_order_id,
                "timestamp": tools.get_cur_timestamp_ms()
            }
            success, error = await self.request("DELETE", "/api/v3/order", params=params, auth=True)
        else:
            params = {
                "symbol": symbol,
                "timestamp": tools.get_cur_timestamp_ms()
            }
            success, error = await self.request("DELETE", "/api/v3/openOrders", params=params, auth=True)

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
                "timestamp": tools.get_cur_timestamp_ms()
            }
            if client_order_id:
                params = {
                    "symbol": symbol,
                    "origClientOrderId": str(client_order_id),
                    "timestamp": tools.get_cur_timestamp_ms()
                }

            success, error = await self.request("GET", "/api/v3/order", params=params, auth=True,logrequestinfo=logrequestinfo)
            successres = success            
            if success:
                if success["status"]=="CANCELED" or success["status"]=="PARTIALLY_CANCELED" :
                    status="已取消"
                elif success["status"]=="FILLED":
                    status="已完成"
                elif success["status"]=="PARTIALLY_FILLED" or success["status"]=="NEW":
                    status="未完成"
                else:
                    status="其他"   
                try:
                    Price = float(success["price"])
                except ValueError:
                    Price = 0.0    
                try:
                    Amount = float(success["origQty"])
                except ValueError:
                    Amount = 0.0 
                try:
                    DealAmount = float(success["executedQty"])
                except ValueError:
                    DealAmount = 0.0 
                try:
                    AvgPrice = 0 if float(success["executedQty"])==0 else float(success["cummulativeQuoteQty"])/float(success["executedQty"])
                except ValueError:
                    AvgPrice = 0.0               
                orderinfo = {
                    "Info"        : successres,         # 请求交易所接口后，交易所接口应答的原始数据，回测时无此属性
                    "Id"          : success["orderId"],        # 交易单唯一标识
                    "Price"       : Price,          # 下单价格，注意市价单的该属性可能为0或者-1
                    "Amount"      : Amount,            # 下单数量，注意市价单的该属性可能为金额并非币数
                    "DealAmount"  : DealAmount,            # 成交数量，如果交易所接口不提供该数据则可能使用0填充
                    "AvgPrice"    : AvgPrice,          # 成交均价，注意有些交易所不提供该数据。不提供、也无法计算得出的情况该属性设置为0
                    "Status"      : status,             # 订单状态，参考常量里的订单状态，例如：ORDER_STATE_CLOSED
                    "Type"        : "Buy" if success["side"]=="BUY" else "Sell",             # 订单类型，参考常量里的订单类型，例如：ORDER_TYPE_BUY
                    "Offset"      : 0,             # 数字货币期货的订单数据中订单的开平仓方向。ORDER_OFFSET_OPEN为开仓方向，ORDER_OFFSET_CLOSE为平仓方向
                    "ContractType" : ""            # 现货订单中该属性为""即空字符串，期货订单该属性为具体的合约代码
                }
            else:
                orderinfo={}       
            return orderinfo, error
    
    async def get_listen_key(self):
        """ Get listen key, start a new user data stream

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        params = {
                "timestamp": tools.get_cur_timestamp_ms()
            }
        success, error = await self.request("POST", "/api/v3/userDataStream",params=params,auth=True)
        return success, error

    async def put_listen_key(self, listen_key):
        """ Keepalive a user data stream to prevent a time out.

        Args:
            listen_key: Listen key.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        params = {
            "timestamp": tools.get_cur_timestamp_ms(),
            "listenKey": listen_key
        }
        success, error = await self.request("PUT", "/api/v3/userDataStream", params=params,auth=True)
        return success, error

    async def delete_listen_key(self, listen_key):
        """ Delete a listen key.

        Args:
            listen_key: Listen key.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        params = {
            "timestamp": tools.get_cur_timestamp_ms(),
            "listenKey": listen_key
        }
        success, error = await self.request("DELETE", "/api/v3/userDataStream", params=params,auth=True)
        return success, error

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
        pdata = {}
        if params:
            pdata.update(params)
        if body:
            pdata.update(body)

        if pdata:
            query = "&".join(["=".join([str(k), str(v)]) for k, v in pdata.items()])
        else:
            query = ""
        if auth and query:
            signature = hmac.new(self._secret_key.encode(), query.encode(), hashlib.sha256).hexdigest()
            query += "&signature={s}".format(s=signature)
        if query:
            url += ("?" + query)

        if not headers:
            headers = {}
        headers["X-MEXC-APIKEY"] = self._access_key
        headers["Content-Type"] = "application/json"

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
        pdata = {}
        if params:
            pdata.update(params)
        if body:
            pdata.update(body)

        if pdata:
            query = "&".join(["=".join([str(k), str(v)]) for k, v in pdata.items()])
        else:
            query = ""
        if auth and query:
            signature = hmac.new(self._secret_key.encode(), query.encode(), hashlib.sha256).hexdigest()
            query += "&signature={s}".format(s=signature)
        if query:
            url += ("?" + query)

        if not headers:
            headers = {}
        headers["X-MEXC-APIKEY"] = self._access_key
        headers["Content-Type"] = "application/json"

        _header, success, error = SyncHttpRequests.fetch(method, url, headers=headers, body=data, timeout=10, verify=False,logrequestinfo=logrequestinfo)
        if rethead:
            return success, error, _header
        else:
            return success, error

class MexcTrade:
    """ Mexc.io Trade module. You can initialize trade object with some attributes in kwargs.

    Attributes:
        account: Account name for this trade exchange.
        strategy: What's name would you want to created for you strategy.
        symbol: Symbol name for your trade.
        host: HTTP request host. (default is "https://api.Mexcio.co")
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
            kwargs["host"] = "https://api.Mexcio.co"
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
        self._platform = MEXC
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
        self._rest_api = MexcRestAPI(self._host, self._access_key, self._secret_key)

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

    @async_method_locker("MexcTrade.order.locker")
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

from quant.utils.web import Websocket
from quant.event import EventTrade, EventKline, EventOrderbook

class MexcAccount:
    """ 
    币安websocket账户信息推送

    """

    def __init__(self,ispublic_to_mq=False,islog=False, **kwargs):
        """Initialize Trade module."""
        
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
            kwargs["host"] = "https://api.mexc.com"
        if not kwargs.get("wss"):
            kwargs["wss"] = "wss://wbs.mexc.com/ws"
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
        self._platform = MEXC
        self._symbols = kwargs["symbols"]
        self._host = kwargs["host"]
        self._wss = kwargs["wss"]
        self._access_key = kwargs["access_key"]
        self._secret_key = kwargs["secret_key"]
        self._asset_update_callback = kwargs.get("asset_update_callback")
        self._order_update_callback = kwargs.get("order_update_callback")
        self._init_success_callback = kwargs.get("init_success_callback")

        #super(MexcAccount, self).__init__(self._wss) #继承自另一个父类，并调用其 __init__() 方法，传递参数 self._wss。

        self._url = self._wss
        

        self._raw_symbols = []
        for symbol in self._symbols:
            self._raw_symbols.append(symbol.replace("_", ""))

        self._listen_key = None  # Listen key for Websocket authentication.

        # Initialize our REST API client.
        self._rest_api = MexcRestAPI(self._host, self._access_key, self._secret_key)


        # Create a loop run task to reset listen key every 30 minutes.
        LoopRunTask.register(self._reset_listen_key, 60 * 30)

        LoopRunTask.register(self.send_ping, 10)

        # Create a coroutine to initialize Websocket connection.
        SingleTask.run(self._init_websocket)
    
    #20秒發送一次心跳
    async def send_ping(self,*args, **kwargs):
        d = {
            "method": "PING",
        }
        if not self._ws:
            logger.error("Websocket connection not yeah!", caller=self)
            return
        await self._ws.send(json.dumps(d))


    @property
    def rest_api(self):
        return self._rest_api

    async def _init_websocket(self):
        """ Initialize Websocket connection.
        """
        # Get listen key first.
        success, error = await self._rest_api.get_listen_key()
        if error:
            e = Error("get listen key failed: {}".format(error))
            logger.error(e, caller=self)
            if self._init_success_callback:
                SingleTask.run(self._init_success_callback, False, e)
            return
        self._listen_key = success["listenKey"]
        uri = "?listenKey=" + self._listen_key
        self._url = urljoin(self._wss, uri)
        self._ws = Websocket(self._url, connected_callback=self.connected_callback,process_callback=self.process)
        self._ws.initialize()
        #self.initialize()

    async def _reset_listen_key(self, *args, **kwargs):
        """ Reset listen key.
        """
        if not self._listen_key:
            logger.error("listen key not initialized!", caller=self)
            return
        await self._rest_api.put_listen_key(self._listen_key)
        logger.info("reset listen key success!", caller=self)

    async def connected_callback(self):
        """ After websocket connection created successfully, pull back all open order information.
        """
        logger.info("Websocket connection authorized successfully.", caller=self)
        d = {
            "method": "SUBSCRIPTION",
            "params": [
            "spot@private.account.v3.api"
            ]
        }
        await self._ws.send(d)
        logger.info("subscribe account success.",d,caller=self)
        d = {
            "method": "SUBSCRIPTION",
            "params": [
                "spot@private.orders.v3.api"
            ]
        }
        await self._ws.send(d)
        logger.info("subscribe orders success.",d,caller=self)
        if self._init_success_callback:
            SingleTask.run(self._init_success_callback, True, None)

    #继承父类方法
    @async_method_locker("MexcAccount.process.locker")
    async def process(self, msg):
        """ Process message that received from Websocket connection.

        Args:
            msg: message received from Websocket connection.
        """
        try:
            e = msg.get("c")
            if e == "spot@private.orders.v3.api":  # Order update.
                if msg["s"] not in self._raw_symbols:
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
            if e == "spot@private.account.v3.api":  # asset update.
                """ [
                    {
                        "a": "USDT",
                        "f": "176.81254174",
                        "l": "201.575"
                    }
                ] """
                
                info = {
                    "platform": self._platform,
                    "account": self._account,
                    "Info":msg,
                    "assets": [{"a":msg["d"].get("a"),
                            "f":float(msg["d"].get("f")),
                            "l":float(msg["d"].get("l"))}],
                    "timestamp": int(msg["d"]["c"]),
                    "update": int(msg["t"])                    
                }
                asset = info
                if self._asset_update_callback:
                    SingleTask.run(self._asset_update_callback, copy.copy(asset))
        except Exception as e:
            pass

#交易所行情数据对象
#通过ws接口获取行情，并通过回调函数传递给策略使用
class MexcMarket:
    """ Binance Market Server.
    1个 ws 连接最多30个订阅

    Attributes:
        ispublic_to_mq:是否将行情推送到行情中心,默认为否
        islog:是否logger输出行情数据,默认为否
        orderbook_update_callback:订单薄数据回调函数
        kline_update_callback:K线数据回调函数
        trade_update_callback:成交数据回调函数
        kwargs:
            platform: Exchange platform name, must be `binance`.
            wss: Exchange Websocket host address, default is `wss://wbs.mexc.com/ws`.
            symbols: Symbol list.
            channels: Channel list, only `orderbook` / `trade` / `kline`/ `tickers` to be enabled.
            orderbook_length: The length of orderbook's data to be published via OrderbookEvent, default is 10.
    """

    def __init__(self,ispublic_to_mq=False,islog=False, orderbook_update_callback=None,kline_update_callback=None,trade_update_callback=None,tickers_update_callback=None, **kwargs):
        self.ispublic_to_mq=ispublic_to_mq
        self.islog=islog
        self._platform = kwargs.get("platform")
        self._account = kwargs.get("account")

        self._wss = kwargs.get("wss", "wss://wbs.mexc.com/ws")
        self._symbols = list(set(kwargs.get("symbols")))
        self._channels = kwargs.get("channels")
        self._orderbook_length = kwargs.get("orderbook_length", 10)

        self._c_to_s = {}
        self._tickers = {}

        url = self._wss
        self._ws = Websocket(url, connected_callback=self.connected_callback,process_callback=self.MexcMarket_process)
        self._ws.initialize()

        kwargs["orderbook_update_callback"] = self.process_orderbook
        kwargs["kline_update_callback"] = self.process_kline
        kwargs["trade_update_callback"] = self.process_trade
        kwargs["tickers_update_callback"] = self.process_tickers

        self._orderbook_update_callback = orderbook_update_callback
        self._kline_update_callback = kline_update_callback
        self._trade_update_callback = trade_update_callback
        self._tickers_update_callback = tickers_update_callback

        LoopRunTask.register(self.send_ping, 10)

    def find_closest(self,num):
        arr = [5, 10, 20]
        closest = arr[0]
        for val in arr:
            if abs(val - num) < abs(closest - num):
                closest = val
        return closest

    #20秒發送一次心跳
    async def send_ping(self,*args, **kwargs):
        d = {
            "method": "PING",
        }
        if not self._ws:
            logger.error("Websocket connection not yeah!", caller=self)
            return
        await self._ws.send(json.dumps(d))


    async def connected_callback(self):
        """ After create connection to Websocket server successfully, we will subscribe orderbook/kline/trade event.
        """
        symbols = []
        for s in self._symbols:
            t = s.replace("_", "")
            symbols.append(t)
        if not symbols:
            logger.warn("symbols not found in config file.", caller=self)
            return
        if not self._channels:
            logger.warn("channels not found in config file.", caller=self)
            return
        for ch in self._channels:
            if ch == "kline":
                symbolsd = []
                for s in symbols:
                    t = "spot@public.kline.v3.api@"+s+"@Min1"
                    symbolsd.append(t)                
                csymbols = tools.cut_list(symbolsd,10)
                for sy in csymbols:
                    d = {
                            "method": "SUBSCRIPTION",
                            "params": sy
                        }
                    await self._ws.send(d)
                    logger.info("subscribe kline success.",d,caller=self)
            elif ch == "orderbook":
                symbolsd = []
                for s in symbols:
                    t = "spot@public.limit.depth.v3.api@"+s+"@"+str(self.find_closest(self._orderbook_length))
                    symbolsd.append(t)  
                csymbols = tools.cut_list(symbolsd,10)
                for sy in csymbols:
                    d = {
                            "method": "SUBSCRIPTION",
                            "params": sy
                        }
                    await self._ws.send(d)
                    logger.info("subscribe orderbook success.",d, caller=self)
            elif ch == "trade":
                symbolsd = []
                for s in symbols:
                    t = "spot@public.deals.v3.api@"+s
                    symbolsd.append(t)  
                csymbols = tools.cut_list(symbolsd,10)
                for sy in csymbols:
                    d = {
                            "method": "SUBSCRIPTION",
                            "params": sy
                        }
                    await self._ws.send(d)
                    logger.info("subscribe trade success.",d, caller=self)
            elif ch == "tickers":
                symbolsd = []
                for s in symbols:
                    t = "spot@public.bookTicker.v3.api@"+s
                    symbolsd.append(t)  
                csymbols = tools.cut_list(symbolsd,10)
                for sy in csymbols:
                    d = {
                            "method": "SUBSCRIPTION",
                            "params": sy
                        }
                    await self._ws.send(d)
                    logger.info("subscribe ticker success.",d, caller=self)
            else:
                logger.error("channel error! channel:", ch, caller=self)

    async def MexcMarket_process(self, msg):
        """Process message that received from Websocket connection.

        Args:
            msg: Message received from Websocket connection.
        """
        topic = msg.get("c")
        data = msg.get("d")
        if data:
            pass
        else:
            #logger.info("返回数据有误:", msg, caller=self)
            return
        if "depth" in topic:
            await self.process_orderbook(data,msg)
        elif "deals" in topic:
            await self.process_trade(data,msg)
        elif "bookTicker" in topic:
            await self.process_tickers(data,msg)
        elif "kline" in topic:
            await self.process_kline(data,msg)

    #加修饰器使得行情信息依次处理,如果之前的数据未处理完新数据直接抛弃，避免数据堆积新旧穿插
    #@async_method_locker("MexcMarket_process_kline",wait=False)
    async def process_kline(self, data,msg):
        """Process kline data and publish KlineEvent."""
        symbol = msg.get("s")
        kline = {
            "Info":msg,
            "platform": self._platform,
            "symbol": symbol,
            "Open": float(data.get("k").get("o")),
            "High": float(data.get("k").get("h")),
            "Low": float(data.get("k").get("l")),
            "Close": float(data.get("k").get("c")),
            "Volume": float(data.get("k").get("v")),
            "Time": msg.get("t"),
        }
        if self.ispublic_to_mq:
            EventKline(**kline).publish()
        if self._kline_update_callback:
            SingleTask.run(self._kline_update_callback, kline)
        if self.islog:
            logger.info("symbol:", symbol, "kline:", kline, caller=self)

    #加修饰器使得行情信息依次处理,如果之前的数据未处理完新数据直接抛弃，避免数据堆积新旧穿插
    #@async_method_locker("MexcMarket_process_orderbook",wait=False)
    async def process_orderbook(self, data,msg):
        """Process orderbook data and publish OrderbookEvent."""
        symbol = msg.get("s")
        #bids = []
        #asks = []
        #for bid in data.get("bids")[:self._orderbook_length]:
        #    bids.append({"Price":float(bid["p"]),"Amount":float(bid["v"])})
        #for ask in data.get("asks")[:self._orderbook_length]:
        #    asks.append({"Price":float(ask["p"]),"Amount":float(ask["v"])})
        orderbook = {
            "Info":msg,
            "platform": self._platform,
            "symbol": symbol,
            "Asks": data.get("asks")[:self._orderbook_length],
            "Bids": data.get("bids")[:self._orderbook_length],
            "Time": msg.get("t")
        }
        if self._orderbook_update_callback:
            SingleTask.run(self._orderbook_update_callback, orderbook)
        if self.ispublic_to_mq:
            EventOrderbook(**orderbook).publish()
        if self.islog:
            logger.info("symbol:", symbol, "orderbook:", orderbook, caller=self)

    #加修饰器使得行情信息依次处理,如果之前的数据未处理完新数据直接抛弃，避免数据堆积新旧穿插
    #@async_method_locker("MexcMarket_process_trade",wait=False)
    async def process_trade(self, data,msg):
        """Process trade data and publish TradeEvent."""
        symbol = msg.get("s")
        trade = {
            "Info":msg,
            "platform": self._platform,
            "symbol": symbol,
            "Type":  0 if data.get("deals")[0].get("S")==1 else 1, 
            "Price": data.get("deals")[0].get("p"),
            "Amount": data.get("deals")[0].get("v"),
            "Time": msg.get("t")
        }
        if self.ispublic_to_mq:
            EventTrade(**trade).publish()
        if self._trade_update_callback:
            SingleTask.run(self._trade_update_callback, trade)
        if self.islog:
            logger.info("symbol:", symbol, "trade:", trade, caller=self)
    
    #加修饰器使得行情信息依次处理,如果之前的数据未处理完新数据直接抛弃，避免数据堆积新旧穿插
    #@async_method_locker("MexcMarket——process_tickers",wait=False)
    async def process_tickers(self, data,msg):
        """Process tickers data and publish TradeEvent."""
        symbol = msg.get("s")
        ticker = {
            "Info":msg,
            "platform": self._platform,
            "symbol": symbol,
            "Sellamount"     : data.get("A"),              
            "Sell"    : data.get("a"),              
            "Buy"     : data.get("b"),              
            "Buyamount"    : data.get("B"),             
            "Time"    : msg.get("t")
        }
        if self.ispublic_to_mq:
            EventTrade(**ticker).publish()
        if self._tickers_update_callback:
            SingleTask.run(self._tickers_update_callback, ticker)
        if self.islog:
            logger.info("symbol:", symbol, "ticker:", ticker, caller=self)
