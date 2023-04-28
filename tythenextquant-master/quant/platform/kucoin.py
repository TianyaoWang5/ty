# -*- coding:utf-8 -*-

"""
Kucoin Trade module.
https://docs.kucoin.com

Author: xunfeng
Date:   2019/08/01
Email:  xunfeng@test.com
"""

import json
import copy
import hmac
import base64
import hashlib
from urllib.parse import urljoin
import time
import math
import traceback

from quant.error import Error
from quant.utils import tools
from quant.utils import logger
from quant.const import KUCOIN
from quant.order import Order
from quant.asset import Asset, AssetSubscribe
from quant.tasks import SingleTask, LoopRunTask
from quant.utils.http_client import AsyncHttpRequests,SyncHttpRequests
from quant.utils.decorator import async_method_locker
from quant.order import ORDER_TYPE_LIMIT, ORDER_TYPE_MARKET
from quant.order import ORDER_ACTION_BUY, ORDER_ACTION_SELL
from quant.order import ORDER_STATUS_SUBMITTED, ORDER_STATUS_PARTIAL_FILLED, ORDER_STATUS_FILLED, \
    ORDER_STATUS_CANCELED, ORDER_STATUS_FAILED, ORDER_STATUS_NONE


__all__ = ("KucoinRestAPI", "KucoinTrade", )


class KucoinRestAPI:
    """ Kucoin REST API client.
    https://api.kucoin.com

    Attributes:
        host: HTTP request host.
        access_key: Account"s ACCESS KEY.
        secret_key: Account"s SECRET KEY.
        passphrase: API KEY passphrase.
    """

    def __init__(self, host, access_key, secret_key, passphrase,platform=None,account=None):
        """initialize REST API client."""
        self._host = host
        self._platform = platform
        self._account = account
        self._access_key = access_key
        self._secret_key = secret_key
        self._passphrase = passphrase
    
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
            exsymbol = symbol.replace('_',"-")
        else:
            success = {}
            error = "请输入要获取交易规则的交易对"
            logger.info(error,caller=self)
            return success, error
        successres=None
        while True:
            try:
                exchangeInfo, error = await self.request("GET", "/api/v2/symbols", "")
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
                    for info in exchangeInfo:
                        if info["symbol"]==exsymbol:
                            minQty=float(info["baseMinSize"])
                            maxQty=float(info["baseMaxSize"])
                            amountSize = int((math.log10(1.1/float(info["baseIncrement"]))))
                            priceSize=int((math.log10(1.1/float(info["priceIncrement"]))))
                            tickSize = float(info["priceIncrement"])
                            minNotional = float(info["minFunds"]) #名义价值
                            success={
                                "Info"    : exchangeInfo if reinfo else "",             #请求交易所接口后，交易所接口应答的原始数据
                                "symbol":symbol,
                                "minQty"    : minQty,              #最小下单量
                                "maxQty"     : maxQty,               #最大下单量
                                "amountSize": amountSize,                    #数量精度位数
                                "priceSize"    : priceSize,               #价格精度位数
                                "tickSize"     : tickSize,               #单挑价格
                                "minNotional"    :minNotional,               #最小订单名义价值
                                "Time"    : tools.get_cur_timestamp_ms()      #毫秒级别时间戳
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

        symbol:BTC_USDT

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if symbol:
            params = {
                "symbol":symbol.replace('_',"-")
            }
        else:
            success = {}
            error = "请输入要获取Ticker的交易对"
            logger.info(error,caller=self)
            return success, error
        successres=None
        while True:
            try:
                success, error = await self.request("GET", "/api/v1/market/stats", params)
                successres = success
                if success:
                    success={
                        "Info"    : success,             #请求交易所接口后，交易所接口应答的原始数据，回测时无此属性
                        "High"    : float(success["high"]),              #最高价，如果交易所接口没有提供24小时最高价则使用卖一价格填充
                        "Low"     : float(success["low"]),               #最低价，如果交易所接口没有提供24小时最低价则使用买一价格填充
                        "Sellamount": 0,
                        "Sell"    : float(success["sell"]),               #卖一价
                        "Buy"     : float(success["buy"]),               #买一价
                        "Buyamount": 0, 
                        "Last"    : float(success["last"]),               #最后成交价
                        "Volume"  : float(success["vol"]),          #最近成交量
                        "Time"    : success["time"]     #毫秒级别时间戳
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

        symbol:BTC_USDT

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if symbol:
            params = {
                "symbol":symbol.replace('_',"-"),
            }
        else:
            success = {}
            error = "请输入要获取depth的交易对"
            logger.info("请输入要获取depth的交易对",caller=self)
            return success, error
        successres=None
        while True:
            try:
                if limit==20:
                    success, error = await self.request("GET", "/api/v1/market/orderbook/level2_20", params)
                else:
                    success, error = await self.request("GET", "/api/v1/market/orderbook/level2_100", params)
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
                if not autotry:
                    logger.info("depth行情更新报错",successres,error,e,caller=self)
                    break
                else:
                    logger.info("depth行情更新报错，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                    await tools.Sleep(sleep/1000)
        return success, error
    
    async def GetTrades(self,symbol=None,limit=100,autotry = False,sleep=100):
        """ 获取当前交易对、合约对应的市场的交易历史（非自己），返回值：Trade结构体数组

        symbol:BTC_USDT

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if symbol:
            params = {
                "symbol":symbol.replace('_',"-"),
            }
        else:
            success = {}
            error = "请输入要获取Trade的交易对"
            logger.info("请输入要获取Trade的交易对",caller=self)
            return success, error
        successres=None
        while True:
            try:
                success, error = await self.request("GET", "/api/v1/market/histories", params)
                successres = success
                if success:
                    aggTrades = []
                    for tr in success:
                        aggTrades.append({
                            "Id":tr["sequence"],
                            "Time":tr["time"],
                            "Price":float(tr["price"]),
                            "Amount":float(tr["size"]),
                            "Type":1 if tr["side"]=="sell" else 0,
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
        获取交易账户资产信息

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        params = {"type":"trade"}
        successres=None
        if basesymbol and quotesymbol:
            while True:
                try:
                    success, error = await self.request("GET", "/api/v1/accounts", params, auth=True)
                    successres = success
                    if success==[]:
                        success={
                            "Info"    : success,
                            "Balance"    : 0,            
                            "FrozenBalance"    : 0,             
                            "Stocks"    : 0,
                            "FrozenStocks"    : 0
                        }
                        break
                    if success:
                        Stocks = 0
                        FrozenStocks = 0
                        Balance = 0
                        FrozenBalance = 0
                        for ac in success:
                            if ac["currency"]==basesymbol:
                                Stocks=float(ac["available"])
                                FrozenStocks=float(ac["holds"])
                            if ac["currency"]==quotesymbol:
                                Balance=float(ac["available"])
                                FrozenBalance=float(ac["holds"])
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
        symbol = symbol.replace('_',"-")
        if ttype == "LIMIT":
            info = {
                "clientOid":tools.get_uuid1(),
                "symbol": symbol,
                "side": "buy",
                "type": "limit",
                "timeInForce": timeInForce,
                "size": quantity,
                "price": price,
            }
        else:
            if ttype=="LIMIT_MAKER":
                info = {
                    "clientOid":tools.get_uuid1(),
                    "symbol": symbol,
                    "side": "buy",
                    "type": "limit",
                    "size": quantity,
                    "price": price,
                    "timeInForce": "GTC",
                    "postOnly":True
                }
        success, error = await self.request("POST", "/api/v1/orders", body=info, auth=True,logrequestinfo=logrequestinfo)
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
        symbol = symbol.replace('_',"-")
        if ttype == "LIMIT":
            info = {
                "clientOid":tools.get_uuid1(),
                "symbol": symbol,
                "side": "sell",
                "type": "limit",
                "timeInForce": timeInForce,
                "size": quantity,
                "price": price,
            }
        else:
            if ttype=="LIMIT_MAKER":
                info = {
                    "clientOid":tools.get_uuid1(),
                    "symbol": symbol,
                    "side": "sell",
                    "type": "limit",
                    "size": quantity,
                    "price": price,
                    "timeInForce": "GTC",
                    "postOnly":True
                }
        success, error = await self.request("POST", "/api/v1/orders", body=info, auth=True,logrequestinfo=logrequestinfo)
        return success, error

    async def CancelOrder(self, symbol, order_id = None):
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
        symbol = symbol.replace('_',"-")
        if order_id :
            success, error = await self.request("DELETE", "/api/v1/orders/"+str(order_id), "", auth=True)
        else:
            params = {
                "symbol": symbol,
                "tradeType":"TRADE"
            }
            success, error = await self.request("DELETE", "/api/v1/orders", params=params, auth=True)

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
            symbol = symbol.replace('_',"-")

        else:
            success = {}
            error = "请输入交易对"
            logger.info("请输入交易对",caller=self)
            return success, error
        if order_id:
            pass
        else:
            success = {}
            error = "请输入交易ID"
            logger.info("请输入交易ID",caller=self)
            return success, error
        if order_id :
            params = ""
            success, error = await self.request("GET", "/api/v1/orders/"+str(order_id), params, auth=True,logrequestinfo=logrequestinfo )
            successres = success    
            if success:
                if not success["isActive"] and success["cancelExist"]:
                    status="已取消"
                elif not success["isActive"] and float(success["dealSize"])==float(success["size"]):
                    status="已完成"
                elif success["isActive"] and (not success["cancelExist"]):
                    status="未完成"
                else:
                    status="其他"  
                try:
                    Price = float(success["price"])
                except ValueError:
                    Price = 0.0    
                try:
                    Amount = float(success["size"])
                except ValueError:
                    Amount = 0.0 
                try:
                    DealAmount = float(success["dealSize"])
                except ValueError:
                    DealAmount = 0.0 
                try:
                    AvgPrice = 0 if float(success["dealSize"]) == 0 else float(success["dealFunds"])/float(success["dealSize"])
                except ValueError:
                    AvgPrice = 0.0             
                orderinfo = {
                    "Info"        : successres,         # 请求交易所接口后，交易所接口应答的原始数据，回测时无此属性
                    "Id"          : success["id"],        # 交易单唯一标识
                    "Price"       : Price,          # 下单价格，注意市价单的该属性可能为0或者-1
                    "Amount"      : Amount,            # 下单数量，注意市价单的该属性可能为金额并非币数
                    "DealAmount"  : DealAmount,            # 成交数量，如果交易所接口不提供该数据则可能使用0填充
                    "AvgPrice"    : AvgPrice,          # 成交均价，注意有些交易所不提供该数据。不提供、也无法计算得出的情况该属性设置为0
                    "Status"      : status,             # 订单状态，参考常量里的订单状态，例如：ORDER_STATE_CLOSED
                    "Type"        : "Buy" if success["side"]=="buy" else "Sell",             # 订单类型，参考常量里的订单类型，例如：ORDER_TYPE_BUY
                    "Offset"      : 0,             # 数字货币期货的订单数据中订单的开平仓方向。ORDER_OFFSET_OPEN为开仓方向，ORDER_OFFSET_CLOSE为平仓方向
                    "ContractType" : ""            # 现货订单中该属性为""即空字符串，期货订单该属性为具体的合约代码
                }
            else:
                orderinfo={}       
            return orderinfo, error

    async def get_sub_users(self):
        """Get the user info of all sub-users via this interface.

        Returns:
            success: Success results, otherwise it"s None.
            error: Error information, otherwise it"s None.
        """
        uri = "/api/v1/sub/user"
        success, error = await self.request("GET", uri, auth=True)
        return success, error

    async def get_accounts(self, account_type=None, currency=None):
        """Get a list of accounts.

        Args:
           account_type: Account type, main or trade.
           currency: Currency name, e.g. BTC, ETH ...

        Returns:
            success: Success results, otherwise it"s None.
            error: Error information, otherwise it"s None.
        """
        uri = "/api/v1/accounts"
        params = {}
        if account_type:
            params["type"] = account_type
        if currency:
            params["currency"] = currency
        success, error = await self.request("GET", uri, params=params, auth=True)
        return success, error

    async def get_account(self, account_id):
        """Information for a single account.

        Args:
           account_id: Account id.

        Returns:
            success: Success results, otherwise it"s None.
            error: Error information, otherwise it"s None.
        """
        uri = "/api/v1/accounts/{}".format(account_id)
        success, error = await self.request("GET", uri, auth=True)
        return success, error

    async def create_account(self, account_type, currency):
        """Create a account.

        Args:
           account_type: Account type, main or trade.
           currency: Currency name, e.g. BTC, ETH ...

        Returns:
            success: Success results, otherwise it"s None.
            error: Error information, otherwise it"s None.
        """
        uri = "/api/v1/accounts"
        body = {
            "type": account_type,
            "currency": currency
        }
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def create_order(self, client_id, side, symbol, order_type, price, size):
        """ Add standard order.

        Args:
            client_id: Unique order id selected by you to identify your order.
            side: Trade side, buy or sell.
            symbol: A valid trading symbol code. e.g. ETH-BTC.
            order_type: Order type, limit or market (default is limit).
            price: Price per base currency.
            size: Amount of base currency to buy or sell.

        Returns:
            success: Success results, otherwise it"s None.
            error: Error information, otherwise it"s None.
        """
        uri = "/api/v1/orders"
        body = {
            "clientOid": client_id,
            "side": side,
            "symbol": symbol,
            "type": order_type,
            "price": price,
            "size": size
        }
        success, error = await self.request("POST", uri, body=body, auth=True)
        return success, error

    async def revoke_order(self, order_id):
        """ Cancel a previously placed order.

        Args:
            order_id: Order ID, unique identifier of an order.

        Returns:
            success: Success results, otherwise it"s None.
            error: Error information, otherwise it"s None.
        """
        uri = "/api/v1/orders/{}".format(order_id)
        success, error = await self.request("DELETE", uri, auth=True)
        return success, error

    async def revoke_orders_all(self, symbol=None):
        """ Attempt to cancel all open orders. The response is a list of ids of the canceled orders.

        Args:
            symbol: A valid trading symbol code. e.g. ETH-BTC.

        Returns:
            success: Success results, otherwise it"s None.
            error: Error information, otherwise it"s None.
        """
        uri = "/api/v1/orders"
        params = {}
        if symbol:
            params["symbol"] = symbol
        success, error = await self.request("DELETE", uri, params=params, auth=True)
        return success, error

    async def get_order_list(self, status="active", symbol=None, order_type=None, start=None, end=None):
        """ Get order information list.

        Args:
            status: Only list orders with a specific status, `active` or `done`, default is `active`.
            symbol: A valid trading symbol code. e.g. ETH-BTC.
            order_type: Order type, limit, market, limit_stop or market_stop.
            start: Start time. Unix timestamp calculated in milliseconds will return only items which were created
                after the start time.
            end: End time. Unix timestamp calculated in milliseconds will return only items which were created
                before the end time.

        Returns:
            success: Success results, otherwise it"s None.
            error: Error information, otherwise it"s None.
        """
        uri = "/api/v1/orders"
        params = {"status": status}
        if symbol:
            params["symbol"] = symbol
        if order_type:
            params["type"] = order_type
        if start:
            params["startAt"] = start
        if end:
            params["endAt"] = end
        success, error = await self.request("GET", uri, params=params, auth=True)
        return success, error

    async def get_order_detail(self, order_id):
        """ Get a single order by order ID.

        Args:
            order_id: Order ID, unique identifier of an order.

        Returns:
            success: Success results, otherwise it"s None.
            error: Error information, otherwise it"s None.
        """
        uri = "/api/v1/orders/{}".format(order_id)
        success, error = await self.request("GET", uri, auth=True)
        return success, error

    async def get_websocket_token(self, private=False):
        """ Get a Websocket token from server.

        Args:
            private: If a private token, default is False.

        Returns:
            success: Success results, otherwise it"s None.
            error: Error information, otherwise it"s None.
        """
        if private:
            uri = "/api/v1/bullet-private"
            success, error = await self.request("POST", uri, auth=True)
        else:
            uri = "/api/v1/bullet-public"
            success, error = await self.request("POST", uri)
        return success, error

    async def get_orderbook(self, symbol, count=20):
        """ Get orderbook information.

        Args:
            symbol: A valid trading symbol code. e.g. ETH-BTC.
            count: Orderbook length, only support 20 or 100.

        Returns:
            success: Success results, otherwise it"s None.
            error: Error information, otherwise it"s None.
        """
        if count == 20:
            uri = "/api/v1/market/orderbook/level2_20?symbol={}".format(symbol)
        else:
            uri = "/api/v2/market/orderbook/level2_100?symbol={}".format(symbol)
        success, error = await self.request("GET", uri)
        return success, error
    
    async def HttpQuery(self,method,url,params=None,data=None,rethead=False,logrequestinfo=False):
        """ 用户自定义HTTP请求接口

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if rethead:
            success, error , header= await self.hrequest(method, url,params,body=data,rethead = rethead,logrequestinfo=logrequestinfo)
            return success, error, header
        else:
            success, error = await self.hrequest(method, url,params,body=data,logrequestinfo=logrequestinfo)
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
            success, error, header = await self.request(method, uri, params, body=data, auth=True,rethead=rethead,logrequestinfo=logrequestinfo)
            return success, error, header
        else:
            success, error = await self.request(method, uri, params, body=data, auth=True,logrequestinfo=logrequestinfo)
            return success, error

    async def request(self, method, uri, params=None, body=None, headers=None, auth=False, rethead=False,logrequestinfo=False):
        """ Do HTTP request.

        Args:
            method: HTTP request method. GET, POST, DELETE, PUT.
            uri: HTTP request uri.
            params: HTTP query params.
            body:   HTTP request body.
            headers: HTTP request headers.
            auth: If this request requires authentication.

        Returns:
            success: Success results, otherwise it"s None.
            error: Error information, otherwise it"s None.
        """
        if params:
            query = "&".join(["{}={}".format(k, params[k]) for k in sorted(params.keys())])
            uri += "?" + query
        url = urljoin(self._host, uri)
        if auth:
            if not headers:
                headers = {}
            timestamp = str(tools.get_cur_timestamp_ms())
            signature = self._generate_signature(timestamp, method, uri, body)
            passphrase = base64.b64encode(hmac.new(self._secret_key.encode("utf-8"), self._passphrase.encode('utf-8'), hashlib.sha256).digest()).decode("utf-8")
            headers["KC-API-KEY"] = self._access_key
            headers["KC-API-SIGN"] = signature
            headers["KC-API-TIMESTAMP"] = timestamp
            headers["KC-API-PASSPHRASE"] = passphrase
            headers["KC-API-KEY-VERSION"] = "2"
            headers["Content-Type"] = "application/json"
        _header, success, error = await AsyncHttpRequests.fetch(method, url, data=body, headers=headers, timeout=10,logrequestinfo=logrequestinfo)
        if rethead:
            if error:
                return None, error,_header
            if success["code"] != "200000":
                return None, success,_header
            return success["data"], error,_header
        else:
            if error:
                return None, error
            if success["code"] != "200000":
                return None, success
            return success["data"], error

    def _generate_signature(self, nonce, method, path, data):
        """Generate the call signature."""
        data = json.dumps(data) if data else ""
        sig_str = "{}{}{}{}".format(nonce, method, path, data)
        m = hmac.new(self._secret_key.encode("utf-8"), sig_str.encode("utf-8"), hashlib.sha256)
        return base64.b64encode(m.digest()).decode("utf-8")
    
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
            success, error, header = self.synhrequest(method, url,params, body=data,rethead=rethead,logrequestinfo=logrequestinfo)
            return success, error, header
        else:
            success, error = self.synhrequest(method, url,params, body=data,logrequestinfo=logrequestinfo)
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
            success, error, header = self.synrequest(method, uri, params, body=data, auth=True,rethead=rethead,logrequestinfo=logrequestinfo)
            return success, error, header
        else:
            success, error = self.synrequest(method, uri, params, body=data, auth=True,logrequestinfo=logrequestinfo)
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
    
    def synrequest(self, method, uri, params=None, body=None, headers=None, auth=False, rethead=False,logrequestinfo=False):
        """ Do HTTP request.

        Args:
            method: HTTP request method. GET, POST, DELETE, PUT.
            uri: HTTP request uri.
            params: HTTP query params.
            body:   HTTP request body.
            headers: HTTP request headers.
            auth: If this request requires authentication.

        Returns:
            success: Success results, otherwise it"s None.
            error: Error information, otherwise it"s None.
        """
        if params:
            query = "&".join(["{}={}".format(k, params[k]) for k in sorted(params.keys())])
            uri += "?" + query
        url = urljoin(self._host, uri)
        if auth:
            if not headers:
                headers = {}
            timestamp = str(tools.get_cur_timestamp_ms())
            signature = self._generate_signature(timestamp, method, uri, body)
            headers["KC-API-KEY"] = self._access_key
            headers["KC-API-SIGN"] = signature
            headers["KC-API-TIMESTAMP"] = timestamp
            headers["KC-API-PASSPHRASE"] = self._passphrase
        _header, success, error = SyncHttpRequests.fetch(method, url, data=body, headers=headers, timeout=10,logrequestinfo=logrequestinfo)
        if rethead:
            if error:
                return None, error,_header
            if success["code"] != "200000":
                return None, success,_header
            return success["data"], error,_header
        else:
            if error:
                return None, error
            if success["code"] != "200000":
                return None, success
            return success["data"], error


class KucoinTrade:
    """ Kucoin Trade module. You can initialize trade object with some attributes in kwargs.

    Attributes:
        account: Account name for this trade exchange.
        strategy: What's name would you want to created for you strategy.
        symbol: Symbol name for your trade.
        host: HTTP request host. (default is "https://api.kucoin.com")
        access_key: Account's ACCESS KEY.
        secret_key: Account's SECRET KEY.
        passphrase: API KEY passphrase.
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
            kwargs["host"] = "https://api.kucoin.com"
        if not kwargs.get("access_key"):
            e = Error("param access_key miss")
        if not kwargs.get("secret_key"):
            e = Error("param secret_key miss")
        if not kwargs.get("passphrase"):
            e = Error("param passphrase miss")
        if e:
            logger.error(e, caller=self)
            if kwargs.get("init_success_callback"):
                SingleTask.run(kwargs["init_success_callback"], False, e)
            return

        self._account = kwargs["account"]
        self._strategy = kwargs["strategy"]
        self._platform = KUCOIN
        self._symbol = kwargs["symbol"]
        self._host = kwargs["host"]
        self._access_key = kwargs["access_key"]
        self._secret_key = kwargs["secret_key"]
        self._passphrase = kwargs["passphrase"]
        self._asset_update_callback = kwargs.get("asset_update_callback")
        self._order_update_callback = kwargs.get("order_update_callback")
        self._init_success_callback = kwargs.get("init_success_callback")
        self._check_order_interval = kwargs.get("check_order_interval", 2)

        self._raw_symbol = self._symbol.replace("/", "-")  # Raw symbol name.

        self._assets = {}  # Asset information. e.g. {"BTC": {"free": "1.1", "locked": "2.2", "total": "3.3"}, ... }
        self._orders = {}  # Order details. e.g. {order_no: order-object, ... }

        # Initialize our REST API client.
        self._rest_api = KucoinRestAPI(self._host, self._access_key, self._secret_key, self._passphrase)

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
        result, error = await self._rest_api.get_order_list(symbol=self._raw_symbol)
        if error:
            e = Error("get open order nos failed: {}".format(error))
            logger.error(e, caller=self)
            if self._init_success_callback:
                SingleTask.run(self._init_success_callback, False, e)
            return
        for item in result["items"]:
            if item["symbol"] != self._raw_symbol:
                continue
            await self._update_order(item)
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
        if action == ORDER_ACTION_BUY:
            action_type = "buy"
        elif action == ORDER_ACTION_SELL:
            action_type = "sell"
        else:
            return None, "action error"
        if order_type == ORDER_TYPE_MARKET:
            order_type_2 = "market"
        elif order_type == ORDER_TYPE_LIMIT:
            order_type_2 = "limit"
        else:
            return None, "order_type error"

        client_id = tools.get_uuid1()
        price = tools.float_to_str(price)
        quantity = tools.float_to_str(quantity)
        success, error = await self._rest_api.create_order(client_id, action_type, self._raw_symbol, order_type_2,
                                                           price, quantity)
        if error:
            return None, error
        order_no = success["orderId"]
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
            _, error = await self._rest_api.revoke_orders_all(self._raw_symbol)
            if error:
                return False, error
            return True, None

        # If len(order_nos) == 1, you will cancel an order.
        if len(order_nos) == 1:
            success, error = await self._rest_api.revoke_order(order_nos[0])
            if error:
                return order_nos[0], error
            else:
                return order_nos[0], None

        # If len(order_nos) > 1, you will cancel multiple orders.
        if len(order_nos) > 1:
            s, e, = [], []
            for order_no in order_nos:
                success, error = await self._rest_api.revoke_order(order_no)
                if error:
                    e.append(error)
                else:
                    s.append(order_no)
            return s, e

    async def get_open_order_nos(self):
        """ Get open order id list.

        Args:
            None.

        Returns:
            order_nos: Open order id list, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        result, error = await self._rest_api.get_order_list(symbol=self._raw_symbol)
        if error:
            return False, error
        order_nos = []
        for item in result["items"]:
            if item["symbol"] != self._raw_symbol:
                continue
            order_nos.append(item["id"])
        return order_nos, None

    async def _check_order_update(self, *args, **kwargs):
        """ Loop run task for check order status.
        """
        order_nos = list(self._orders.keys())
        if not order_nos:
            return
        for order_no in order_nos:
            success, error = await self._rest_api.get_order_detail(order_no)
            if error:
                return
            await self._update_order(success)

    @async_method_locker("KucoinTrade.order.locker")
    async def _update_order(self, order_info):
        """ Update order object.

        Args:
            order_info: Order information.
        """
        if not order_info:
            return

        order_no = order_info["id"]
        size = float(order_info["size"])
        deal_size = float(order_info["dealSize"])
        order = self._orders.get(order_no)
        if not order:
            info = {
                "platform": self._platform,
                "account": self._account,
                "strategy": self._strategy,
                "order_no": order_no,
                "action": ORDER_ACTION_BUY if order_info["side"] == "buy" else ORDER_ACTION_SELL,
                "symbol": self._symbol,
                "price": order_info["price"],
                "quantity": order_info["size"],
                "remain": order_info["size"],
                "avg_price": order_info["price"]
            }
            order = Order(**info)
            self._orders[order_no] = order

        if order_info["isActive"]:
            if size == deal_size:
                status = ORDER_STATUS_SUBMITTED
            else:
                status = ORDER_STATUS_PARTIAL_FILLED
        else:
            if size == deal_size:
                status = ORDER_STATUS_FILLED
            else:
                status = ORDER_STATUS_CANCELED

        if status != order.status:
            order.status = status
            order.remain = size - deal_size
            order.ctime = order_info["createdAt"]
            order.utime = tools.get_cur_timestamp_ms()
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

from quant.event import EventTrade, EventKline, EventOrderbook
from quant.utils.web import Websocket


class KucoinAccount:
    """ 
    Kucoinwebsocket账户信息推送

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
            kwargs["host"] = "https://api.kucoin.com"
        if not kwargs.get("wss"):
            kwargs["wss"] = "wss://stream.binance.com:9443"
        if not kwargs.get("access_key"):
            e = Error("param access_key miss")
        if not kwargs.get("secret_key"):
            e = Error("param secret_key miss")
        if not kwargs.get("passphrase"):
            e = Error("param passphrase miss")
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
        self._platform = KUCOIN
        self._symbols = kwargs["symbols"]
        self._host = kwargs["host"]
        self._wss = kwargs["wss"]
        self._access_key = kwargs["access_key"]
        self._secret_key = kwargs["secret_key"]
        self._passphrase = kwargs["passphrase"]
        self._asset_update_callback = kwargs.get("asset_update_callback")
        self._order_update_callback = kwargs.get("order_update_callback")
        self._init_success_callback = kwargs.get("init_success_callback")

        #super(KucoinAccount, self).__init__(self._wss)

        self._raw_symbols = []
        for symbol in self._symbols:
            self._raw_symbols.append(symbol.replace("_", "-"))

        self._listen_key = None  # Listen key for Websocket authentication.

        # Initialize our REST API client.
        self._rest_api = KucoinRestAPI(self._host, self._access_key, self._secret_key,self._passphrase)


        LoopRunTask.register(self.send_heartbeat_msg, 15)

        # Create a coroutine to initialize Websocket connection.
        SingleTask.run(self._init_websocket)


    @property
    def rest_api(self):
        return self._rest_api

    async def _init_websocket(self):
        """ Initialize Websocket connection.
        """
        # Get listen key first.
        success, error = await self._rest_api.get_websocket_token(private=True)
        if error:
            logger.error("get websocket token error!", caller=self)
            return
        url = "{}?token={}".format(success["instanceServers"][0]["endpoint"], success["token"])
        self._ws = Websocket(url, self.connected_callback, self.process)
        self._ws.initialize()

    async def send_heartbeat_msg(self, *args, **kwargs):
        request_id = await self.generate_request_id()
        data = {
            "id": request_id,
            "type": "ping"
        }
        if not self._ws:
            logger.error("Websocket connection not yeah!", caller=self)
            return
        await self._ws.send(data)

    @async_method_locker("KucoinAccount_generate_request_id")
    async def generate_request_id(self):
        self._request_id = tools.get_cur_timestamp_ms()
        return self._request_id

    async def connected_callback(self):
        """ After websocket connection created successfully, pull back all open order information.
        """
        logger.info("Websocket connection authorized successfully.", caller=self)
        request_id = await self.generate_request_id()
        d={
            "id":request_id,
            "type":"subscribe",
            "topic":"/account/balance",
            "privateChannel":True,
            "response":True
        }
        await self._ws.send(d)
        logger.info("subscribe balance success.",d, caller=self)
        request_id = await self.generate_request_id()
        d={
            "id":request_id,
            "type":"subscribe",
            "topic":"/spotMarket/tradeOrdersV2",
            "privateChannel":True,
            "response":True
        }
        await self._ws.send(d)
        logger.info("subscribe tradeOrdersV2 success.",d, caller=self)
        if self._init_success_callback:
            SingleTask.run(self._init_success_callback, True, None)


    @async_method_locker("KucoinAccount.process.locker")
    async def process(self, msg):
        """ Process message that received from Websocket connection.

        Args:
            msg: message received from Websocket connection.
        """
        try:
            e = msg.get("subject")
            if e == "orderChange":  # Order update.
                if msg["data"]["symbol"] not in self._raw_symbols:
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
            if e == "account.balance":  # asset update.
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
                    "assets": [{"a":msg["data"].get("currency"),
                            "f":float(msg["data"].get("available")),
                            "l":float(msg["data"].get("hold"))}],
                    "timestamp": int(msg["time"]),
                    "update": int(msg["time"])                
                }
                asset = info
                if self._asset_update_callback:
                    SingleTask.run(self._asset_update_callback, copy.copy(asset))
        except Exception as e:
            pass


class KucoinMarket:
    """ Kucoin Market Server.

    Attributes:
        kwargs:
            platform: Exchange platform name, must be `kucoin`.
            host: Exchange Websocket host address, default is "https://api.kucoin.com".
            symbols: symbol list, Future instrument_id list.
            channels: channel list, only `orderbook` , `kline` and `trade` to be enabled.
            orderbook_length: The length of orderbook's data to be published via OrderbookEvent, default is 20.
            orderbook_interval: The interval time to fetch a orderbook information, default is 2 seconds.
    """

    def __init__(self,ispublic_to_mq=False,islog=False, orderbook_update_callback=None,kline_update_callback=None,trade_update_callback=None,tickers_update_callback=None, **kwargs):
        self.ispublic_to_mq=ispublic_to_mq
        self.islog=islog
        self._platform = kwargs.get("platform")
        self._account = kwargs.get("account")
        
        self._host = kwargs.get("host", "https://api.kucoin.com")
        self._symbols = list(set(kwargs.get("symbols")))
        self._channels = kwargs.get("channels")
        self._orderbook_length = kwargs.get("orderbook_length", 20)  # only support for 20 or 100.

        if self._orderbook_length != 20:
            self._orderbook_length = 50

        self._request_id = 0  # Unique request id for pre request.
        self._orderbooks = {}  # Orderbook data, e.g. {"symbol": {"bids": {"price": quantity, ...}, "asks": {...}, timestamp: 123, "sequence": 123}}
        self._last_publish_ts = 0  # The latest publish timestamp for OrderbookEvent.
        self._ws = None  # Websocket object.

        # REST API client.
        self._rest_api = KucoinRestAPI(self._host, None, None, None)
        
        self._orderbook_update_callback = orderbook_update_callback
        self._kline_update_callback = kline_update_callback
        self._trade_update_callback = trade_update_callback
        self._tickers_update_callback = tickers_update_callback

        kwargs["orderbook_update_callback"] = self.process_orderbook
        kwargs["kline_update_callback"] = self.process_kline
        kwargs["trade_update_callback"] = self.process_trade
        kwargs["tickers_update_callback"] = self.process_tickers

        SingleTask.run(self._initialize)
        
    def find_closest(self,num):
        arr = [5, 50]
        closest = arr[0]
        for val in arr:
            if abs(val - num) < abs(closest - num):
                closest = val
        return closest

    async def _initialize(self):
        """Initialize."""
        
        # Create Websocket connection.
        success, error = await self._rest_api.get_websocket_token()
        if error:
            logger.error("get websocket token error!", caller=self)
            return
        url = "{}?token={}".format(success["instanceServers"][0]["endpoint"], success["token"])
        self._ws = Websocket(url, self.connected_callback, self.process)
        self._ws.initialize()
        LoopRunTask.register(self.send_heartbeat_msg, 15)

    async def connected_callback(self):
        """ After create connection to Websocket server successfully, we will subscribe orderbook/kline/trade event.
        """
        symbols = []
        for s in self._symbols:
            t = s.replace("_", "-")
            symbols.append(t)
        if not symbols:
            logger.warn("symbols not found in config file.", caller=self)
            return
        if not self._channels:
            logger.warn("channels not found in config file.", caller=self)
            return
        for ch in self._channels:
            request_id = await self.generate_request_id()
            if ch == "kline":
                symbols = []
                for s in self._symbols:
                    t = s.replace("_", "-")+"_1min"
                    symbols.append(t)                
                d = {
                    "id": request_id,
                    "type": "subscribe",
                    "topic": "/market/candles:" + ",".join(symbols),
                    "privateChannel": False,
                    "response": True
                }
                await self._ws.send(d)
                logger.info("subscribe kline success.",d,caller=self)
            elif ch == "orderbook":
                d = {
                    "id": request_id,
                    "type": "subscribe",
                    "topic": "/spotMarket/level2Depth"+str(self.find_closest(self._orderbook_length))+":" + ",".join(symbols),
                    "response": True
                }
                await self._ws.send(d)
                logger.info("subscribe orderbook success.",d, caller=self)
            elif ch == "trade":
                d = {
                    "id": request_id,
                    "type": "subscribe",
                    "topic": "/market/match:" + ",".join(symbols),
                    "privateChannel": False,
                    "response": True
                }
                await self._ws.send(d)
                logger.info("subscribe trade success.",d, caller=self)
            elif ch == "tickers":
                d = {
                    "id": request_id,
                    "type": "subscribe",
                    "topic": "/market/ticker:" + ",".join(symbols),
                    "privateChannel": False,
                    "response": True
                }
                await self._ws.send(d)
                logger.info("subscribe ticker success.",d, caller=self)
            else:
                logger.error("channel error! channel:", ch, caller=self)

    async def send_heartbeat_msg(self, *args, **kwargs):
        request_id = await self.generate_request_id()
        data = {
            "id": request_id,
            "type": "ping"
        }
        if not self._ws:
            logger.error("Websocket connection not yeah!", caller=self)
            return
        await self._ws.send(data)

    @async_method_locker("_generate_request_id")
    async def generate_request_id(self):
        self._request_id = tools.get_cur_timestamp_ms()
        return self._request_id

    async def process(self, msg):
        """ Process message that received from Websocket connection.

        Args:
            msg: Message received from Websocket connection.
        """
        #logger.debug("msg:", msg, caller=self)

        topic = msg.get("topic", "")
        data = msg.get("data")
        if data:
            pass
        else:
            #logger.info("返回数据有误:", msg, caller=self)
            return
        if "level2" in topic:
            await self.process_orderbook(data,msg)
        elif "match" in topic:
            await self.process_trade(data,msg)
        elif "ticker" in topic:
            await self.process_tickers(data,msg)
        elif "candles" in topic:
            await self.process_kline(data,msg)

    #加修饰器使得行情信息依次处理,如果之前的数据未处理完新数据直接抛弃，避免数据堆积新旧穿插
    #@async_method_locker("KucoinMarket_process_trade",wait=False)
    async def process_trade(self, data,msg):
        """ Deal with trade data, and publish trade message to EventCenter via TradeEvent.

        Args:
            data: Newest trade data.
        """
        symbol = data.get("symbol")
        trade = {
            "Info":msg,
            "platform": self._platform,
            "symbol": symbol,
            "Type":  0 if data["side"]=="buy" else 1, 
            "Price": data.get("price"),
            "Amount": data.get("size"),
            "Time": int(data.get("time")/1000000)
        }
        if self._trade_update_callback:
            SingleTask.run(self._trade_update_callback, trade)
        if self.ispublic_to_mq:
            EventTrade(**trade).publish()
        if self.islog:
            logger.info("symbol:", symbol, "trade:", trade, caller=self)

    #加修饰器使得行情信息依次处理,如果之前的数据未处理完新数据直接抛弃，避免数据堆积新旧穿插
    #@async_method_locker("KucoinMarket_process_orderbook",wait=False)
    async def process_orderbook(self, data,msg):
        """ Deal with orderbook message that updated.

        Args:
            data: Newest orderbook data.
        """
        symbol = msg.get("topic").split(":")[1]
        # bids = []
        # asks = []
        # for bid in data.get("bids")[:self._orderbook_length]:
        #     bids.append({"Price":float(bid[0]),"Amount":float(bid[1])})
        # for ask in data.get("asks")[:self._orderbook_length]:
        #     asks.append({"Price":float(ask[0]),"Amount":float(ask[1])})
        orderbook = {
            "Info":msg,
            "platform": self._platform,
            "symbol": symbol,
            "Asks": data.get("asks")[:self._orderbook_length],
            "Bids": data.get("bids")[:self._orderbook_length],
            "Time": data.get("timestamp")
        }
        if self._orderbook_update_callback:
            SingleTask.run(self._orderbook_update_callback, orderbook)
        if self.ispublic_to_mq:
            EventOrderbook(**orderbook).publish()
        if self.islog:
            logger.info("symbol:", symbol, "orderbook:", orderbook, caller=self)

    #加修饰器使得行情信息依次处理,如果之前的数据未处理完新数据直接抛弃，避免数据堆积新旧穿插
    #@async_method_locker("KucoinMarket_process_kline",wait=False)
    async def process_kline(self, data,msg):
        """ Deal with 1min kline data, and publish kline message to EventCenter via KlineEvent.

        Args:
            data: Newest kline data.
        """
        symbol = data.get("symbol")
        kline = {
            "Info":msg,
            "platform": self._platform,
            "symbol": symbol,
            "Open": float(data["candles"][1]),
            "High": float(data["candles"][3]),
            "Low": float(data["candles"][4]),
            "Close": float(data["candles"][2]),
            "Volume": float(data["candles"][5]),
            "Time": int(data.get("time")/1000000),
        }
        if self._kline_update_callback:
            SingleTask.run(self._kline_update_callback, kline)
        if self.ispublic_to_mq:
            EventKline(**kline).publish()
        if self.islog:
            logger.info("symbol:", symbol, "kline:", kline, caller=self)

    #加修饰器使得行情信息依次处理,如果之前的数据未处理完新数据直接抛弃，避免数据堆积新旧穿插
    #@async_method_locker("KucoinMarket_process_tickers",wait=False)
    async def process_tickers(self, data,msg):
        """Process aggTrade data and publish TradeEvent."""
        symbol = msg.get("topic").split(":")[1]
        ticker = {
            "Info":msg,
            "platform": self._platform,
            "symbol": symbol,
            "Sellamount"     : data.get("bestAskSize"),              
            "Sell"    : data.get("bestAsk"),              
            "Buy"     : data.get("bestBid"),              
            "Buyamount"    : data.get("bestBidSize"),             
            "Time"    : tools.get_cur_timestamp_ms() 
        }
        if self._tickers_update_callback:
            SingleTask.run(self._tickers_update_callback, ticker)
        if self.ispublic_to_mq:
            EventTrade(**ticker).publish()
        if self.islog:
            logger.info("symbol:", symbol, "ticker:", ticker, caller=self)

