# -*- coding:utf-8 -*-

"""
Okx Trade module.
https://www.okx.me/docs/zh/

Author: xunfeng
Date:   2019/01/19
Email:  xunfeng@test.com
"""

import time
import json
import copy
import hmac
import zlib
import base64
import hashlib
import math
import traceback

from urllib.parse import urljoin

from quant.error import Error
from quant.utils import tools
from quant.utils import logger
from quant.const import OKX
from quant.order import Order
from quant.tasks import SingleTask
from quant.utils.websocket import Websocket
from quant.asset import Asset, AssetSubscribe
from quant.utils.decorator import async_method_locker
from quant.utils.http_client import AsyncHttpRequests
from quant.order import ORDER_ACTION_BUY, ORDER_ACTION_SELL
from quant.order import ORDER_TYPE_LIMIT, ORDER_TYPE_MARKET
from quant.order import ORDER_STATUS_SUBMITTED, ORDER_STATUS_PARTIAL_FILLED, ORDER_STATUS_FILLED, \
    ORDER_STATUS_CANCELED, ORDER_STATUS_FAILED
from quant.utils.http_client import AsyncHttpRequests
from quant.utils.http_client import SyncHttpRequests

__all__ = ("OkxRestAPI", "OkxTrade", )


class OkxRestAPI:
    """ Okx REST API client.
    实盘API交易地址如下：

    REST：https://www.okx.com/
    WebSocket公共频道：wss://ws.okx.com:8443/ws/v5/public
    WebSocket私有频道：wss://ws.okx.com:8443/ws/v5/private
    AWS 地址如下：

    REST：https://aws.okx.com
    WebSocket公共频道：wss://wsaws.okx.com:8443/ws/v5/public
    WebSocket私有频道：wss://wsaws.okx.com:8443/ws/v5/private

    Attributes:
        host: HTTP request host.
        access_key: Account's ACCESS KEY.
        secret_key: Account's SECRET KEY.
        passphrase: API KEY Passphrase.
    """

    def __init__(self, host, access_key, secret_key, passphrase,platform=None,account=None):
        """initialize."""
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
                params = {"instType": "SPOT"}
                exchangeInfo, error = await self.request("GET", "/api/v5/public/instruments", params)
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
                    for info in exchangeInfo["data"]:
                        if info["instId"]==exsymbol:
                            minQty=float(info["minSz"])
                            maxQty=float(info["maxTriggerSz"])
                            amountSize = int((math.log10(1.1/float(info["lotSz"]))))
                            priceSize=int((math.log10(1.1/float(info["tickSz"]))))
                            tickSize = float(info["tickSz"])
                            minNotional = 0 #名义价值
                            success={
                                "Info"    : exchangeInfo if reinfo else "",             #请求交易所接口后，交易所接口应答的原始数据
                                "symbol":symbol,
                                "minQty"    : minQty,              #最小下单量
                                "maxQty"     : maxQty,               #最大下单量
                                "amountSize": amountSize,                    #数量精度位数
                                "priceSize"    : priceSize,               #价格精度位数
                                "tickSize"     : tickSize,               #单挑价格
                                "minNotional"    :minNotional,               #最小订单名义价值
                                "Time"    : tools.get_cur_timestamp_ms()     #毫秒级别时间戳
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

    async def GetTicker(self, symbol=None, autotry=False, sleep=100):
        """ 获取当前交易对、合约对应的市场当前行情，返回值:Ticker结构体。

        instId:BTC-USD-SWAP\BTC-USDT\BTC-USD-180216

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if symbol:
            symbol = symbol.replace('_',"-")
            params = {
                "instId":symbol
            }
        else:
            success = {}
            error = "请输入要获取Ticker的交易对"
            logger.info(error,caller=self)
            return success, error
        successres=None
        while True:
            try:
                success, error = await self.request("GET", "/api/v5/market/ticker", params)
                successres = success
                if success:
                    success={
                        "Info"    : success,             #请求交易所接口后，交易所接口应答的原始数据，回测时无此属性
                        "High"    : float(success["data"][0]["high24h"]),              #最高价，如果交易所接口没有提供24小时最高价则使用卖一价格填充
                        "Low"     : float(success["data"][0]["low24h"]),               #最低价，如果交易所接口没有提供24小时最低价则使用买一价格填充
                        "Sellamount": float(success["data"][0]["askSz"]),
                        "Sell"    : float(success["data"][0]["askPx"]),               #卖一价
                        "Buy"     : float(success["data"][0]["bidPx"]),               #买一价
                        "Buyamount": float(success["data"][0]["bidSz"]),
                        "Last"    : float(success["data"][0]["last"]),               #最后成交价
                        "Volume"  : float(success["data"][0]["volCcy24h"]),          #最近24h成交量(基础币)
                        "Time"    : int(success["data"][0]["ts"])      #毫秒级别时间戳
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

    async def GetDepth(self, symbol=None,limit = 100, autotry= False, sleep=100):
        """ 获取当前交易对、合约对应的市场的订单薄数据，返回值：Depth结构体。

        symbol:BTC-USDT

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        # ts = tools.get_cur_timestamp_ms()
        if symbol:
            symbol = symbol.replace('_',"-")
            params = {
                "instId":symbol,
                "sz":limit
            }
        else:
            success = {}
            error = "请输入要获取depth的交易对"
            logger.info("请输入要获取depth的交易对",caller=self)
            return success, error
        successres=None
        while True:
            try:
                # if limit<100:
                #     params = {
                #         "instId":symbol
                #     }
                #     success, error = await self.request("GET", "/api/v5/market/books-lite", params)
                # else:
                #     success, error = await self.request("GET", "/api/v5/market/books", params)
                success, error = await self.request("GET", "/api/v5/market/books", params)
                successres=success
                if success:
                    Bids=[]
                    Asks=[]
                    for dp in success["data"][0]["bids"][0:limit]:
                        Bids.append({"Price":float(dp[0]),"Amount":float(dp[1])})
                    for dp in success["data"][0]["asks"][0:limit]:
                        Asks.append({"Price":float(dp[0]),"Amount":float(dp[1])})
                    success={
                        "Info"    : success,
                        "Asks"    : Asks,             #卖单数组，MarketOrder数组,按价格从低向高排序
                        "Bids"    : Bids,             #买单数组，MarketOrder数组,按价格从高向低排序
                        "Time"    : int(success["data"][0]["ts"])      #毫秒级别时间戳
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
    
    async def GetTrades(self, symbol=None,limit=100, autotry=False, sleep=100):
        """ 获取当前交易对、合约对应的市场的交易历史（非自己），返回值：Trade结构体数组
        # 未归集数据
        symbol:BTC-USDT

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if symbol:
            symbol = symbol.replace('_',"-")
            params = {
                "instId":symbol,
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
                success, error = await self.request("GET", "/api/v5/market/trades", params)
                successres=success
                if success:
                    aggTrades = []
                    for tr in success["data"]:
                        aggTrades.append({
                            "Id":int(tr["tradeId"]),
                            "Time":int(tr["ts"]),
                            "Price":float(tr["px"]),
                            "Amount":float(tr["sz"]),
                            "Type": 1 if tr["side"] == "sell" else 0,
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
        """ 获取单一币种的资产信息.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        params = ""
        successres=None
        if basesymbol and quotesymbol:
            while True:
                try:
                    success, error = await self.request("GET", "/api/v5/account/balance", params, auth=True)
                    successres = success
                    if success:
                        Stocks = 0
                        FrozenStocks = 0
                        Balance = 0
                        FrozenBalance = 0
                        for ac in success["data"][0]["details"]:
                            if ac["ccy"] == basesymbol:
                                Stocks = float(ac["availBal"])
                                FrozenStocks = float(ac["ordFrozen"])
                            if ac["ccy"] == quotesymbol:
                                Balance = float(ac["availBal"])
                                FrozenBalance = float(ac["ordFrozen"])
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

    async def Buy(self, symbol, price:str, quantity:str, ttype="LIMIT", timeInForce="GTC", resptype="RESULT",logrequestinfo=False):
        """ Create an order.
        Args:
            {
                "instId":"BTC-USDT",
                "tdMode":"cash",
                "clOrdId":"b15",
                "side":"buy",
                "ordType":"limit",
                "px":"2.15",
                "sz":"2"
            }
        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        
        ---Binance---,
        类型	            强制要求的参数
        LIMIT	            timeInForce, quantity, price
        MARKET	            quantity
        LIMIT_MAKER	        quantity, price
            ttype,
                LIMIT 限价单
                MARKET 市价单
                LIMIT_MAKER 限价只挂单
            timeInForce:
                GTC:成交为止,
                IOC:无法立即成交的部分就撤销,
                FOK:无法全部立即成交就撤销
        ---Okx---,
        类型	            强制要求的参数
        limit	            quantity, price
        market	            quantity
        post_only	        quantity, price
        fok	                quantity, price
        ioc	                quantity, price
            ordType:
                market:市价单
                limit:限价单
                post_only:只做maker单
                fok:全部成交或立即取消
                ioc:立即成交并取消剩余
        """
        if ttype == "LIMIT" and timeInForce == "GTC":   # 常用
            ttype = "limit"
        elif ttype == "LIMIT" and timeInForce == "IOC":   # 常用
            ttype = "ioc"
        elif ttype == "LIMIT" and timeInForce == "FOK":   # 常用
            ttype = "fok"
        elif ttype == "MARKET":
            ttype = "market"
        elif ttype == "LIMIT_MAKER":   # 常用
            ttype = "post_only"
        else:
            ttype = "no type"
        symbol = symbol.replace('_',"-")
        if ttype != "no type":
            info = {
                "instId": symbol,
                "tdMode": "cash",
                "side": "buy",
                "ordType": ttype,
                "sz": quantity, # 精度问题，科学计数法问题
                "px": price, # 精度问题，科学计数法问题
            }
        else:
            logger.info("参数ttype错误,ttype:",ttype,caller=self)
            error = "参数ttype错误!"
            return None, error
        success, error = await self.request("POST", "/api/v5/trade/order", body=info, auth=True,logrequestinfo=logrequestinfo)
        return success, error
    
    async def Sell(self, symbol, price:str, quantity:str, ttype="LIMIT", timeInForce="GTC", resptype="RESULT",logrequestinfo=False):
        """ Create an order.
        Args:
            {
                "instId":"BTC-USDT",
                "tdMode":"cash",
                "clOrdId":"b15",
                "side":"sell",
                "ordType":"limit",
                "px":"2.15",
                "sz":"2"
            }

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if ttype == "LIMIT" and timeInForce == "GTC":   # 常用
            ttype = "limit"
        elif ttype == "LIMIT" and timeInForce == "IOC":   # 常用
            ttype = "ioc"
        elif ttype == "LIMIT" and timeInForce == "FOK":   # 常用
            ttype = "fok"
        elif ttype == "MARKET":
            ttype = "market"
        elif ttype == "LIMIT_MAKER":   # 常用
            ttype = "post_only"
        else:
            ttype = "no type"
        symbol = symbol.replace('_',"-")
        if ttype != "no type":
            info = {
                "instId": symbol,
                "tdMode": "cash",
                "side": "sell",
                "ordType": ttype,
                "sz": quantity, # 精度问题，科学计数法问题
                "px": price, # 精度问题，科学计数法问题
            }
        else:
            logger.info("参数ttype错误,ttype:",ttype,caller=self)
            error = "参数ttype错误!"
            return None, error
        success, error = await self.request("POST", "/api/v5/trade/order", body=info, auth=True,logrequestinfo=logrequestinfo)
        return success, error

    async def CancelOrder(self, symbol, order_id = None, client_order_id = None):
        """ Cancelling an unfilled order.
        Args:
            symbol: Symbol name, e.g. BTCUSDT.
            order_id: Order id.
            client_order_id: Client order id.
            当传入2种ID时,以order_id为准
            当没有传入订单ID时,撤销单一交易对的所有订单
            [{
                "instId": "BTC-USDT",
                "ordId": "12312"
            }, {
                "instId": "BTC-USDT",
                "ordId": "1212"
            }]
        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        symbol = symbol.replace('_',"-")
        if order_id or client_order_id:
            bodys = {
                "instId": symbol,
                "ordId": str(order_id),
                "clOrdId": str(client_order_id),
            }
            success, error = await self.request("POST", "/api/v5/trade/cancel-order", body=bodys, auth=True)
        else:
            param_pending = {
                "instType": "SPOT",
                "instId": symbol,
            }
            # 获取未成交订单
            success_orders, error = await self.request("GET", "/api/v5/trade/orders-pending", params=param_pending, auth=True)
            if error:
                return success_orders, error
            bodys=[]
            for row in success_orders["data"]:
                if(row["instId"]==symbol):
                    bodys.append({
                        "instId": symbol,
                        "ordId": row["ordId"]
                    })
            success, error = await self.request("POST", "/api/v5/trade/cancel-batch-orders", body=bodys, auth=True)
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
        if order_id or client_order_id:
            pass
        else:
            success = {}
            error = "请输入交易ID"
            logger.info("请输入交易ID",caller=self)
            return success, error
        if order_id or client_order_id :
            params = {
                        "instId":symbol,
                        "ordId":str(order_id),
                    }
            if client_order_id:
                params = {
                            "instId":symbol,
                            "clOrdId":str(order_id),
                        }
            success, error = await self.request("GET", "/api/v5/trade/order", params, auth=True,logrequestinfo=logrequestinfo )
            successres = success    
            data = success["data"][0]
            if data:
                if data["state"] == "canceled":
                    status="已取消"
                elif data["state"] =="filled":
                    status="已完成"
                elif data["state"] == "partially_filled" or data["state"] == "live":
                    status="未完成"
                else:
                    status="其他"  
                try:
                    Price = float(data["px"])
                except ValueError:
                    Price = 0.0    
                try:
                    Amount = float(data["sz"])
                except ValueError:
                    Amount = 0.0 
                try:
                    DealAmount = float(data["accFillSz"])
                except ValueError:
                    DealAmount = 0.0 
                try:
                    AvgPrice = float(data["avgPx"])
                except ValueError:
                    AvgPrice = 0.0            
                orderinfo = {
                    "Info"        : successres,         # 请求交易所接口后，交易所接口应答的原始数据，回测时无此属性
                    "Id"          : data["ordId"],        # 交易单唯一标识
                    "Price"       : Price,          # 下单价格，注意市价单的该属性可能为0或者-1
                    "Amount"      : Amount,            # 下单数量，注意市价单的该属性可能为金额并非币数
                    "DealAmount"  : DealAmount,            # 成交数量，如果交易所接口不提供该数据则可能使用0填充
                    "AvgPrice"    : AvgPrice,          # 成交均价，注意有些交易所不提供该数据。不提供、也无法计算得出的情况该属性设置为0
                    "Status"      : status,             # 订单状态，参考常量里的订单状态，例如：ORDER_STATE_CLOSED
                    "Type"        : "Buy" if data["side"]=="buy" else "Sell",             # 订单类型，参考常量里的订单类型，例如：ORDER_TYPE_BUY
                    "Offset"      : 0,             # 数字货币期货的订单数据中订单的开平仓方向。ORDER_OFFSET_OPEN为开仓方向，ORDER_OFFSET_CLOSE为平仓方向
                    "ContractType" : ""            # 现货订单中该属性为""即空字符串，期货订单该属性为具体的合约代码
                }
            else:
                orderinfo={}       
            return orderinfo, error

    async def get_user_account(self):
        """ Get account asset information.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        result, error = await self.request("GET", "/api/v5/accounts", auth=True)
        return result, error

    async def create_order(self, action, symbol, price, quantity, order_type=ORDER_TYPE_LIMIT):
        """ Create an order.
        Args:
            action: Action type, `BUY` or `SELL`.
            symbol: Trading pair, e.g. BTCUSDT.
            price: Order price.
            quantity: Order quantity.
            order_type: Order type, `MARKET` or `LIMIT`.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        info = {
            "side": "buy" if action == ORDER_ACTION_BUY else "sell",
            "instrument_id": symbol,
            "margin_trading": 1
        }
        if order_type == ORDER_TYPE_LIMIT:
            info["type"] = "limit"
            info["price"] = price
            info["size"] = quantity
        elif order_type == ORDER_TYPE_MARKET:
            info["type"] = "market"
            if action == ORDER_ACTION_BUY:
                info["notional"] = quantity  # buy price.
            else:
                info["size"] = quantity  # sell quantity.
        else:
            logger.error("order_type error! order_type:", order_type, caller=self)
            return None
        result, error = await self.request("POST", "/api/spot/v3/orders", body=info, auth=True)
        return result, error

    async def revoke_order(self, symbol, order_no):
        """ Cancelling an unfilled order.
        Args:
            symbol: Trading pair, e.g. BTCUSDT.
            order_no: order ID.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        body = {
            "instrument_id": symbol
        }
        uri = "/api/spot/v3/cancel_orders/{order_no}".format(order_no=order_no)
        result, error = await self.request("POST", uri, body=body, auth=True)
        if error:
            return order_no, error
        if result["result"]:
            return order_no, None
        return order_no, result

    async def revoke_orders(self, symbol, order_nos):
        """ Cancelling multiple open orders with order_id，Maximum 10 orders can be cancelled at a time for each
            trading pair.

        Args:
            symbol: Trading pair, e.g. BTCUSDT.
            order_nos: order IDs.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if len(order_nos) > 10:
            logger.warn("only revoke 10 orders per request!", caller=self)
        body = [
            {
                "instrument_id": symbol,
                "order_ids": order_nos[:10]
            }
        ]
        result, error = await self.request("POST", "/api/spot/v3/cancel_batch_orders", body=body, auth=True)
        return result, error

    async def get_open_orders(self, symbol, limit=100):
        """ Get order details by order ID.

        Args:
            symbol: Trading pair, e.g. BTCUSDT.
            limit: order count to return, max is 100, default is 100.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        uri = "/api/spot/v3/orders_pending"
        params = {
            "instrument_id": symbol,
            "limit": limit
        }
        result, error = await self.request("GET", uri, params=params, auth=True)
        return result, error

    async def get_order_status(self, symbol, order_no):
        """ Get order status.
        Args:
            symbol: Trading pair, e.g. BTCUSDT.
            order_no: order ID.

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        params = {
            "instrument_id": symbol
        }
        uri = "/api/spot/v3/orders/{order_no}".format(order_no=order_no)
        result, error = await self.request("GET", uri, params=params, auth=True)
        return result, error

    async def HttpQuery(self,method,url,params=None,rethead=False,logrequestinfo=False):
        """ 用户自定义HTTP请求接口

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if rethead:
            success, error , header= await self.hrequest(method, url,params,rethead = rethead,logrequestinfo=logrequestinfo)
            return success, error, header
        else:
            success, error = await self.hrequest(method, url,params,logrequestinfo=logrequestinfo)
            return success, error

    async def exchangeIO(self,method,uri, params,rethead=False,logrequestinfo=False):
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
            success, error, header = await self.request(method, uri, params, auth=True,rethead=rethead,logrequestinfo=logrequestinfo)
            return success, error, header
        else:
            success, error = await self.request(method, uri, params, auth=True,logrequestinfo=logrequestinfo)
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
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if params:
            query = "&".join(["{}={}".format(k, params[k]) for k in sorted(params.keys())])
            uri += "?" + query
        url = urljoin(self._host, uri)

        if auth:
            timestamp = str(time.time()).split(".")[0] + "." + str(time.time()).split(".")[1][:3]
            if body:
                body = json.dumps(body)
            else:
                body = ""
            message = str(timestamp) + str.upper(method) + uri + str(body)
            mac = hmac.new(bytes(self._secret_key, encoding="utf8"), bytes(message, encoding="utf-8"),
                           digestmod="sha256")
            d = mac.digest()
            sign = base64.b64encode(d)

            if not headers:
                headers = {}
            headers["Content-Type"] = "application/json"
            headers["OK-ACCESS-KEY"] = self._access_key.encode().decode()
            headers["OK-ACCESS-SIGN"] = sign.decode()
            headers["OK-ACCESS-TIMESTAMP"] = str(timestamp)
            headers["OK-ACCESS-PASSPHRASE"] = self._passphrase
        _header, success, error = await AsyncHttpRequests.fetch(method, url, body=body, headers=headers, timeout=10,logrequestinfo=logrequestinfo)
        if rethead:
            return success, error, _header
        else:
            return success, error

    async def hrequest(self, method, url, params=None, body=None, headers=None, auth=False, rethead=False,logrequestinfo=False):
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
        if params:
            query = "&".join(["{}={}".format(k, params[k]) for k in sorted(params.keys())])
            url += "?" + query

        # if auth:
        #     timestamp = str(time.time()).split(".")[0] + "." + str(time.time()).split(".")[1][:3]
        #     if body:
        #         body = json.dumps(body)
        #     else:
        #         body = ""
        #     message = str(timestamp) + str.upper(method) + url + str(body)
        #     mac = hmac.new(bytes(self._secret_key, encoding="utf8"), bytes(message, encoding="utf-8"),
        #                    digestmod="sha256")
        #     d = mac.digest()
        #     sign = base64.b64encode(d)

        #     if not headers:
        #         headers = {}
        #     headers["Content-Type"] = "application/json"
        #     headers["OK-ACCESS-KEY"] = self._access_key.encode().decode()
        #     headers["OK-ACCESS-SIGN"] = sign.decode()
        #     headers["OK-ACCESS-TIMESTAMP"] = str(timestamp)
        #     headers["OK-ACCESS-PASSPHRASE"] = self._passphrase
        _header, success, error = await AsyncHttpRequests.fetch(method, url, body=body, headers=headers, timeout=10,logrequestinfo=logrequestinfo)
        if rethead:
            return success, error, _header
        else:
            return success, error

    def synHttpQuery(self,method,url,params=None, rethead=False,logrequestinfo=False):
        """ 用户自定义httpt请求同步接口

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if rethead:
            success, error, header = self.synhrequest(method, url,params,rethead=rethead,logrequestinfo=logrequestinfo)
            return success, error, header
        else:
            success, error = self.synhrequest(method, url,params,logrequestinfo=logrequestinfo)
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
            success, error, header = self.synrequest(method, uri, params, auth=True,rethead=rethead,logrequestinfo=logrequestinfo)
            return success, error, header
        else:
            success, error = self.synrequest(method, uri, params, auth=True,logrequestinfo=logrequestinfo)
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
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if params:
            query = "&".join(["{}={}".format(k, params[k]) for k in sorted(params.keys())])
            uri += "?" + query
        url = urljoin(self._host, uri)

        if auth:
            timestamp = str(time.time()).split(".")[0] + "." + str(time.time()).split(".")[1][:3]
            if body:
                body = json.dumps(body)
            else:
                body = ""
            message = str(timestamp) + str.upper(method) + uri + str(body)
            mac = hmac.new(bytes(self._secret_key, encoding="utf8"), bytes(message, encoding="utf-8"),
                           digestmod="sha256")
            d = mac.digest()
            sign = base64.b64encode(d)

            if not headers:
                headers = {}
            headers["Content-Type"] = "application/json"
            headers["OK-ACCESS-KEY"] = self._access_key.encode().decode()
            headers["OK-ACCESS-SIGN"] = sign.decode()
            headers["OK-ACCESS-TIMESTAMP"] = str(timestamp)
            headers["OK-ACCESS-PASSPHRASE"] = self._passphrase
        _header, success, error = SyncHttpRequests.fetch(method, url, headers=headers, timeout=10, verify=False,logrequestinfo=logrequestinfo)
        if rethead:
            return success, error, _header
        else:
            return success, error

    def synhrequest(self, method, url, params=None, body=None, headers=None, auth=False, rethead=False,logrequestinfo=False):
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
        if params:
            query = "&".join(["{}={}".format(k, params[k]) for k in sorted(params.keys())])
            url += "?" + query

        # if auth:
        #     timestamp = str(time.time()).split(".")[0] + "." + str(time.time()).split(".")[1][:3]
        #     if body:
        #         body = json.dumps(body)
        #     else:
        #         body = ""
        #     message = str(timestamp) + str.upper(method) + uri + str(body)
        #     mac = hmac.new(bytes(self._secret_key, encoding="utf8"), bytes(message, encoding="utf-8"),
        #                    digestmod="sha256")
        #     d = mac.digest()
        #     sign = base64.b64encode(d)

        #     if not headers:
        #         headers = {}
        #     headers["Content-Type"] = "application/json"
        #     headers["OK-ACCESS-KEY"] = self._access_key.encode().decode()
        #     headers["OK-ACCESS-SIGN"] = sign.decode()
        #     headers["OK-ACCESS-TIMESTAMP"] = str(timestamp)
        #     headers["OK-ACCESS-PASSPHRASE"] = self._passphrase
        _header, success, error = SyncHttpRequests.fetch(method, url, headers=headers, timeout=10, verify=False,logrequestinfo=logrequestinfo)
        if rethead:
            return success, error, _header
        else:
            return success, error

class OkxTrade(Websocket):
    """ Okx Trade module. You can initialize trade object with some attributes in kwargs.

    Attributes:
        account: Account name for this trade exchange.
        strategy: What's name would you want to created for you strategy.
        symbol: Symbol name for your trade.
        host: HTTP request host. (default "https://www.okx.com")
        wss: Websocket address. (default "wss://real.okx.com:8443")
        access_key: Account's ACCESS KEY.
        secret_key Account's SECRET KEY.
        passphrase API KEY Passphrase.
        asset_update_callback: You can use this param to specific a async callback function when you initializing Trade
            object. `asset_update_callback` is like `async def on_asset_update_callback(asset: Asset): pass` and this
            callback function will be executed asynchronous when received AssetEvent.
        order_update_callback: You can use this param to specific a async callback function when you initializing Trade
            object. `order_update_callback` is like `async def on_order_update_callback(order: Order): pass` and this
            callback function will be executed asynchronous when some order state updated.
        init_success_callback: You can use this param to specific a async callback function when you initializing Trade
            object. `init_success_callback` is like `async def on_init_success_callback(success: bool, error: Error, **kwargs): pass`
            and this callback function will be executed asynchronous after Trade module object initialized successfully.
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
            kwargs["host"] = "https://www.okx.com"
        if not kwargs.get("wss"):
            kwargs["wss"] = "wss://real.okx.com:8443"
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
        self._platform = OKX
        self._symbol = kwargs["symbol"]
        self._host = kwargs["host"]
        self._wss = kwargs["wss"]
        self._access_key = kwargs["access_key"]
        self._secret_key = kwargs["secret_key"]
        self._passphrase = kwargs["passphrase"]
        self._asset_update_callback = kwargs.get("asset_update_callback")
        self._order_update_callback = kwargs.get("order_update_callback")
        self._init_success_callback = kwargs.get("init_success_callback")

        self._raw_symbol = self._symbol.replace("/", "-")
        self._order_channel = "spot/order:{symbol}".format(symbol=self._raw_symbol)

        url = self._wss + "/ws/v3"
        super(OkxTrade, self).__init__(url, send_hb_interval=5)
        self.heartbeat_msg = "ping"

        self._assets = {}  # Asset object. e.g. {"BTC": {"free": "1.1", "locked": "2.2", "total": "3.3"}, ... }
        self._orders = {}  # Order objects. e.g. {"order_no": Order, ... }

        # Initializing our REST API client.
        self._rest_api = OkxRestAPI(self._host, self._access_key, self._secret_key, self._passphrase)

        # Subscribing AssetEvent.
        if self._asset_update_callback:
            AssetSubscribe(self._platform, self._account, self.on_event_asset_update)

        self.initialize()

    @property
    def assets(self):
        return copy.copy(self._assets)

    @property
    def orders(self):
        return copy.copy(self._orders)

    @property
    def rest_api(self):
        return self._rest_api

    async def connected_callback(self):
        """After websocket connection created successfully, we will send a message to server for authentication."""
        timestamp = str(time.time()).split(".")[0] + "." + str(time.time()).split(".")[1][:3]
        message = str(timestamp) + "GET" + "/users/self/verify"
        mac = hmac.new(bytes(self._secret_key, encoding="utf8"), bytes(message, encoding="utf8"), digestmod="sha256")
        d = mac.digest()
        signature = base64.b64encode(d).decode()
        data = {
            "op": "login",
            "args": [self._access_key, self._passphrase, timestamp, signature]
        }
        await self.ws.send_json(data)

    @async_method_locker("OkxTrade.process_binary.locker")
    async def process_binary(self, raw):
        """ Process binary message that received from websocket.

        Args:
            raw: Binary message received from websocket.

        Returns:
            None.
        """
        decompress = zlib.decompressobj(-zlib.MAX_WBITS)
        msg = decompress.decompress(raw)
        msg += decompress.flush()
        msg = msg.decode()
        if msg == "pong":
            return
        logger.debug("msg:", msg, caller=self)
        msg = json.loads(msg)

        # Authorization message received.
        if msg.get("event") == "login":
            if not msg.get("success"):
                e = Error("Websocket connection authorized failed: {}".format(msg))
                logger.error(e, caller=self)
                SingleTask.run(self._init_success_callback, False, e)
                return
            logger.info("Websocket connection authorized successfully.", caller=self)

            # Fetch orders from server. (open + partially filled)
            order_infos, error = await self._rest_api.get_open_orders(self._raw_symbol)
            if error:
                e = Error("get open orders error: {}".format(msg))
                SingleTask.run(self._init_success_callback, False, e)
                return

            if len(order_infos) > 100:
                logger.warn("order length too long! (more than 100)", caller=self)
            for order_info in order_infos:
                order_info["ctime"] = order_info["created_at"]
                order_info["utime"] = order_info["timestamp"]
                self._update_order(order_info)

            # Subscribe order channel.
            data = {
                "op": "subscribe",
                "args": [self._order_channel]
            }
            await self.ws.send_json(data)
            return

        # Subscribe response message received.
        if msg.get("event") == "subscribe":
            if msg.get("channel") == self._order_channel:
                SingleTask.run(self._init_success_callback, True, None)
            else:
                e = Error("subscribe order event error: {}".format(msg))
                SingleTask.run(self._init_success_callback, False, e)
            return

        # Order update message received.
        if msg.get("table") == "spot/order":
            for data in msg["data"]:
                data["ctime"] = data["timestamp"]
                data["utime"] = data["last_fill_time"]
                self._update_order(data)

    async def create_order(self, action, price, quantity, order_type=ORDER_TYPE_LIMIT):
        """ Create an order.

        Args:
            action: Trade direction, `BUY` or `SELL`.
            price: Price of each contract.
            quantity: The buying or selling quantity.
            order_type: Order type, `MARKET` or `LIMIT`.

        Returns:
            order_no: Order ID if created successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        price = tools.float_to_str(price)
        quantity = tools.float_to_str(quantity)
        result, error = await self._rest_api.create_order(action, self._raw_symbol, price, quantity, order_type)
        if error:
            return None, error
        if not result["result"]:
            return None, result
        return result["order_id"], None

    async def revoke_order(self, *order_nos):
        """ Revoke (an) order(s).

        Args:
            order_nos: Order id list, you can set this param to 0 or multiple items. If you set 0 param, you can cancel
                all orders for this symbol(initialized in Trade object). If you set 1 param, you can cancel an order.
                If you set multiple param, you can cancel multiple orders. Do not set param length more than 100.

        Returns:
            Success or error, see bellow.

        NOTEs:
            DO NOT INPUT MORE THAT 10 ORDER NOs, you can invoke many times.
        """
        # If len(order_nos) == 0, you will cancel all orders for this symbol(initialized in Trade object).
        if len(order_nos) == 0:
            order_infos, error = await self._rest_api.get_open_orders(self._raw_symbol)
            if error:
                return False, error
            if len(order_infos) > 100:
                logger.warn("order length too long! (more than 100)", caller=self)
            for order_info in order_infos:
                order_no = order_info["order_id"]
                _, error = await self._rest_api.revoke_order(self._raw_symbol, order_no)
                if error:
                    return False, error
            return True, None

        # If len(order_nos) == 1, you will cancel an order.
        if len(order_nos) == 1:
            success, error = await self._rest_api.revoke_order(self._raw_symbol, order_nos[0])
            if error:
                return order_nos[0], error
            else:
                return order_nos[0], None

        # If len(order_nos) > 1, you will cancel multiple orders.
        if len(order_nos) > 1:
            success, error = [], []
            for order_no in order_nos:
                _, e = await self._rest_api.revoke_order(self._raw_symbol, order_no)
                if e:
                    error.append((order_no, e))
                else:
                    success.append(order_no)
            return success, error

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
            return None, error
        else:
            if len(success) > 100:
                logger.warn("order length too long! (more than 100)", caller=self)
            order_nos = []
            for order_info in success:
                order_nos.append(order_info["order_id"])
            return order_nos, None

    def _update_order(self, order_info):
        """ Order update.

        Args:
            order_info: Order information.

        Returns:
            None.
        """
        order_no = str(order_info["order_id"])
        state = order_info["state"]
        remain = float(order_info["size"]) - float(order_info["filled_size"])
        ctime = tools.utctime_str_to_mts(order_info["ctime"])
        utime = tools.utctime_str_to_mts(order_info["utime"])

        if state == "-2":
            status = ORDER_STATUS_FAILED
        elif state == "-1":
            status = ORDER_STATUS_CANCELED
        elif state == "0":
            status = ORDER_STATUS_SUBMITTED
        elif state == "1":
            status = ORDER_STATUS_PARTIAL_FILLED
        elif state == "2":
            status = ORDER_STATUS_FILLED
        else:
            logger.error("status error! order_info:", order_info, caller=self)
            return None

        order = self._orders.get(order_no)
        if order:
            order.remain = remain
            order.status = status
            order.price = order_info["price"]
        else:
            info = {
                "platform": self._platform,
                "account": self._account,
                "strategy": self._strategy,
                "order_no": order_no,
                "action": ORDER_ACTION_BUY if order_info["side"] == "buy" else ORDER_ACTION_SELL,
                "symbol": self._symbol,
                "price": order_info["price"],
                "quantity": order_info["size"],
                "remain": remain,
                "status": status,
                "avg_price": order_info["price"]
            }
            order = Order(**info)
            self._orders[order_no] = order
        order.ctime = ctime
        order.utime = utime

        SingleTask.run(self._order_update_callback, copy.copy(order))

        if status in [ORDER_STATUS_FAILED, ORDER_STATUS_CANCELED, ORDER_STATUS_FILLED]:
            self._orders.pop(order_no)

    async def on_event_asset_update(self, asset: Asset):
        """ Asset event data callback.

        Args:
            asset: Asset object callback from EventCenter.

        Returns:
            None.
        """
        self._assets = asset
        SingleTask.run(self._asset_update_callback, asset)

from quant.utils.web import Websocket
from quant.event import EventTrade, EventKline, EventOrderbook
from quant.tasks import SingleTask,LoopRunTask

class OkxAccount:
    """ 
    Okx,websocket账户信息推送
    
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
            kwargs["host"] = "https://www.okx.com"
        if not kwargs.get("wss"):
            kwargs["wss"] = "wss://ws.okx.com:8443/ws/v5/private"
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
        self._platform = OKX
        self._symbols = kwargs["symbols"]
        self._host = kwargs["host"]
        self._wss = kwargs["wss"]
        self._access_key = kwargs["access_key"]
        self._secret_key = kwargs["secret_key"]
        self._passphrase = kwargs["passphrase"]
        self.alltostrategy = kwargs.get("alltostrategy",False)

        
        self._asset_update_callback = kwargs.get("asset_update_callback")
        self._order_update_callback = kwargs.get("order_update_callback")
        self._allinfo_callback = kwargs.get("_allinfo_callback")   
             
        self.login = False
        self.isalive = False

        url = self._wss
        self._ws = Websocket(url, connected_callback=self.send_auth,process_callback=self.process)
        self._ws.initialize()

        self._raw_symbols = []
        for symbol in self._symbols:
            self._raw_symbols.append(symbol.replace("_", "-"))

        LoopRunTask.register(self.send_ping, 20)


        # Initialize our REST API client.
        #self._rest_api = BybitRestAPI(self._host, self._access_key, self._secret_key)


        # Create a loop run task to reset listen key every 30 minutes.
        #LoopRunTask.register(self._reset_listen_key, 60 * 30)

        # Create a coroutine to initialize Websocket connection.
        #SingleTask.run(self._init_websocket)

    #20秒發送一次心跳
    async def send_ping(self,*args, **kwargs):
        data = "ping"
        if not self._ws:
            self.isalive = False
            logger.error("Websocket connection not yeah!", caller=self)
            return
        await self._ws.send(data)

    async def send_auth(self):        
        timestamp = str(time.time()).split(".")[0] + "." + str(time.time()).split(".")[1][:3]
        message = str(timestamp) + "GET" + "/users/self/verify"
        mac = hmac.new(bytes(self._secret_key, encoding="utf8"), bytes(message, encoding="utf8"), digestmod="sha256")
        d = mac.digest()
        signature = base64.b64encode(d).decode()
        data = {
            "op": "login",
            "args": [{"apiKey":self._access_key,
                       "passphrase":self._passphrase, 
                       "timestamp":timestamp, 
                       "sign":signature}]
        }
        await self._ws.send(json.dumps(data))
        while not self.login:
            await tools.Sleep(100)
        data = {
            "op": "subscribe",
            "args": [{
                "channel": "account",
            }]
        }
        await self._ws.send(json.dumps(data))
        logger.info("subscribe account success.",data,caller=self)


        data = {
            "op": "subscribe",
            "args": [{
                "channel": "orders",
                "instType": "SPOT",
            }]
        }
        await self._ws.send(json.dumps(data))
        logger.info("subscribe orders success.",data,caller=self)


    @async_method_locker("OkxAccount.process")
    async def process(self, msg):
        """ Process message that received from Websocket connection.

        Args:
            msg: message received from Websocket connection.
        """
        try :
            if not self.login:
                if msg.get("event")=="login":
                    if msg.get("code")=="0":
                        self.login = True
            if self.alltostrategy:
                info = {
                    "platform": self._platform,
                    "account": self._account,
                    "strategy": self._strategy,
                    "Info":msg,
                }
                SingleTask.run(self._allinfo_callback, copy.copy(info))
                return
            e = msg.get("arg").get("channel")
            if e == "orders":  # Order update.
                if msg.get("data"):
                    info = {
                        "platform": self._platform,
                        "account": self._account,
                        "strategy": self._strategy,
                        "Info":msg,
                    }
                    order = info
                    if self._order_update_callback:
                        SingleTask.run(self._order_update_callback, copy.copy(order))
            if e == "account":  # asset update.
                """ [
                    {
                        "a": "USDT",
                        "f": "176.81254174",
                        "l": "201.575"
                    }
                ] """
                data = msg.get("data")[0].get("details")
                if data:
                    pass
                else:
                    return
                assets = []
                for ba in data:
                    assets.append({"a":ba.get("ccy"),
                            "f":float(ba.get("availBal")),
                            "l":float(ba.get("ordFrozen"))})
                info = {
                    "platform": self._platform,
                    "account": self._account,
                    "Info":msg,
                    "assets": assets,
                    "timestamp": int(msg.get("data")[0].get("uTime")),
                    "update": int(msg.get("data")[0].get("uTime"))                    
                }
                asset = info
                if self._asset_update_callback:
                    SingleTask.run(self._asset_update_callback, copy.copy(asset))
            self.isalive = True
        except Exception as e:
            pass

class OkxMarket:
    """ OKEx Market Server.
    实盘API交易地址如下：

    REST：https://www.okx.com/
    WebSocket公共频道：wss://ws.okx.com:8443/ws/v5/public
    WebSocket私有频道：wss://ws.okx.com:8443/ws/v5/private
    AWS 地址如下：

    REST：https://aws.okx.com
    WebSocket公共频道：wss://wsaws.okx.com:8443/ws/v5/public
    WebSocket私有频道：wss://wsaws.okx.com:8443/ws/v5/private

    Attributes:
        ispublic_to_mq:是否将行情推送到行情中心,默认为否
        islog:是否logger输出行情数据,默认为否
        orderbook_update_callback:订单薄数据回调函数
        kline_update_callback:K线数据回调函数
        trade_update_callback:成交数据回调函数

        kwargs:
            platform: Exchange platform name, must be `okex` or `okex_margin`.
            host: Exchange Websocket host address, default is `wss://ws.okx.com:8443/ws/v5/public`.
            symbols: symbol list, OKEx Future instrument_id list.
            channels: channel list, only `orderbook`, `kline` and `trade` to be enabled.
            orderbook_length: The length of orderbook's data to be published via OrderbookEvent, default is 10.
    """

    def __init__(self,ispublic_to_mq=False,islog=False, orderbook_update_callback=None,kline_update_callback=None,trade_update_callback=None,tickers_update_callback=None, **kwargs):
        self.ispublic_to_mq=ispublic_to_mq
        self.islog=islog
        self._platform = kwargs["platform"]
        self._account = kwargs.get("account")

        self._wss = kwargs.get("wss", "wss://ws.okx.com:8443/ws/v5/public")
        self._symbols = list(set(kwargs.get("symbols")))
        self._channels = kwargs.get("channels")
        self._orderbook_length = kwargs.get("orderbook_length", 5)

        self._orderbooks = {}  # 订单薄数据 {"symbol": {"bids": {"price": quantity, ...}, "asks": {...}}}

        url = self._wss
        self._ws = Websocket(url, connected_callback=self.connected_callback,process_callback=self.process)
        self._ws.initialize()

        kwargs["orderbook_update_callback"] = self.publish_orderbook
        kwargs["kline_update_callback"] = self.process_kline
        kwargs["trade_update_callback"] = self.process_trade
        kwargs["tickers_update_callback"] = self.process_tickers

        self._orderbook_update_callback = orderbook_update_callback
        self._kline_update_callback = kline_update_callback
        self._trade_update_callback = trade_update_callback
        self._tickers_update_callback = tickers_update_callback

        LoopRunTask.register(self.send_heartbeat_msg, 10)
    
    def find_closest(self,num):
        arr = [5, 10, 20]
        closest = arr[0]
        for val in arr:
            if abs(val - num) < abs(closest - num):
                closest = val
        return closest

    async def connected_callback(self):
        """After create Websocket connection successfully, we will subscribing orderbook/trade/kline."""
        for ch in self._channels:
            if ch == "orderbook":
                topics=[]
                args = []
                if self._orderbook_length>5:
                    books="books"
                else:
                    books = "books5"
                if self._orderbook_length==1:
                    books="bbo-tbt"                
                for symbol in self._symbols:
                    symbol=symbol.replace("_","-")
                    self._orderbooks[symbol]={}
                    args.append({"channel":books,"instId":symbol})
                culist = tools.cut_list(args,10)
                for arg in culist:
                    topics={
                            "op": "subscribe",
                            "args": arg
                            }
                    await self._ws.send(json.dumps(topics))
                    logger.info("subscribe books success.",topics,caller=self)
            elif ch == "trade":
                topics=[]
                args = []
                for symbol in self._symbols:
                    symbol=symbol.replace("_","-")
                    args.append({"channel":"trades","instId":symbol})
                culist = tools.cut_list(args,10)
                for arg in culist:
                    topics={
                            "op": "subscribe",
                            "args": arg
                            }
                    await self._ws.send(json.dumps(topics))
                    logger.info("subscribe trades success.",topics,caller=self)

            elif ch == "kline":
                topics=[]
                args = []
                for symbol in self._symbols:
                    symbol=symbol.replace("_","-")
                    args.append({"channel":"candle1m","instId":symbol})
                culist = tools.cut_list(args,10)
                for arg in culist:
                    topics={
                            "op": "subscribe",
                            "args": arg
                            }
                    await self._ws.send(json.dumps(topics))
                    logger.info("subscribe candle1m success.",topics,caller=self)

            elif ch == "tickers":
                topics=[]
                args = []
                for symbol in self._symbols:
                    symbol=symbol.replace("_","-")
                    args.append({"channel":"tickers","instId":symbol})
                culist = tools.cut_list(args,10)
                for arg in culist:
                    topics={
                            "op": "subscribe",
                            "args": arg
                            }
                    await self._ws.send(json.dumps(topics))
                    logger.info("subscribe tickers success.",topics,caller=self)
            #实时一档深度
            elif ch == "bookone":
                topics=[]
                args = []
                for symbol in self._symbols:
                    symbol=symbol.replace("_","-")
                    args.append({"channel":"bbo-tbt","instId":symbol})
                culist = tools.cut_list(args,10)
                for arg in culist:
                    topics={
                            "op": "subscribe",
                            "args": arg
                            }
                    await self._ws.send(json.dumps(topics))
                    logger.info("subscribe bookone success.",topics,caller=self)

            else:
                logger.error("channel error! channel:", ch, caller=self)

    async def send_heartbeat_msg(self, *args, **kwargs):
        data = "ping"
        if not self._ws:
            logger.error("Websocket connection not yeah!", caller=self)
            return
        await self._ws.send(data)

    async def process(self, msg):
        """Process message that received from Websocket connection.

        Args:
            msg: Message received from Websocket connection.
        """
        if not isinstance(msg, dict):
            return
        arg=msg.get("arg")
        channel = arg.get("channel")
        
        # logger.debug("msg:", msg, caller=self)
        data = msg.get("data")

        symbol = arg.get("instId")
        if data:
            pass
        else:
            #logger.info("返回数据有误:", msg, caller=self)
            return

        if "books" in channel :
            if msg.get("action") == "snapshot":
                for d in data:
                    await self.process_orderbook_partial(symbol,d,msg)
            elif msg.get("action") == "update":
                for d in data:
                    await self.deal_orderbook_update(symbol,d,msg)
            else:
                if not msg.get("action",False):
                    await self.process_orderbook(symbol,data,msg)
                else:
                    logger.warn("unhandle msg:", msg, caller=self)
        elif "bbo-tbt" in channel:
            await self.process_bookone(symbol, data,msg)
        elif "candle1m" in channel:
            await self.process_kline(symbol, data,msg)
        elif "trades" in channel:
            await self.process_trade(symbol, data,msg)
        elif "tickers" in channel:
            await self.process_tickers(symbol, data,msg)

    #@async_method_locker("process_orderbook_partial",wait=False)
    async def process_orderbook_partial(self,symbol, data,msg):
        """
        全量订单薄数据
        Process orderbook partical data.
        """
        if symbol not in self._orderbooks:
            return
        asks = data.get("asks")
        bids = data.get("bids")
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
        self._orderbooks[symbol]["Time"] = data.get("ts")

    @async_method_locker("OkxMarket_deal_orderbook_update",wait=True)
    async def deal_orderbook_update(self, symbol, data,msg):
        """
        增量订单薄数据
        Process orderbook update data.
        """
        asks = data.get("asks")
        bids = data.get("bids")
        timestamp = data.get("ts")

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

        await self.publish_orderbook(symbol,msg)
    
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

    #@async_method_locker("OkxMarket_publish_orderbook",wait=True)
    async def publish_orderbook(self, symbol,msg,*args, **kwargs):
        """Publish OrderbookEvent."""
        ob = copy.copy(self._orderbooks[symbol])
        if not ob["Asks"] or not ob["Bids"]:
            logger.warn("symbol:", symbol, "Asks:", ob["Asks"], "Bids:", ob["Bids"], caller=self)
            return

        ask_keys = sorted(list(ob["Asks"].keys()))
        bid_keys = sorted(list(ob["Bids"].keys()), reverse=True)
        if float(ask_keys[0]) <= float(bid_keys[0]):
            logger.warn("Ok深度信息合并有误,尝试重启wss", symbol, "ask1:", ask_keys[0], "bid1:", bid_keys[0], caller=self)
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
            "Time": int(ob["Time"])
        }
        if self._orderbook_update_callback:
            SingleTask.run(self._orderbook_update_callback, orderbook)
        if self.ispublic_to_mq:
            EventOrderbook(**orderbook).publish()
        if self.islog:
            logger.debug("symbol:", symbol, "orderbook:", orderbook, caller=self)

    #@async_method_locker("OkxMarket_process_trade",wait=False)
    async def process_trade(self, symbol, data,msg):
        """Process trade data and publish TradeEvent."""
        data = data[0]
        trade = {
            "Info":msg,
            "platform": self._platform,
            "symbol": symbol,
            "Type":  0 if data["side"]=="buy" else 1, 
            "Price": data.get("px"),
            "Amount": data.get("sz"),
            "Time": int(data.get("ts"))
        }
        if self.ispublic_to_mq:
            EventTrade(**trade).publish()
        if self._trade_update_callback:
            SingleTask.run(self._trade_update_callback, trade)
        if self.islog:
            logger.info("symbol:", symbol, "trade:", trade, caller=self)
    
    #@async_method_locker("OkxMarket_process_kline",wait=False)
    async def process_kline(self, symbol, data,msg):
        """Process kline data and publish KlineEvent."""
        data=data[0]
        timestamp = int(data[0])
        _open = float(data[1])
        high = float(data[2])
        low = float(data[3])
        close = float(data[4])
        volume = float(data[5])

        kline = {
            "Info":msg,
            "platform": self._platform,
            "symbol": symbol,
            "Open": _open,
            "High": high,
            "Low": low,
            "Close": close,
            "Volume": volume,
            "Time": timestamp,
        }
        if self.ispublic_to_mq:
            EventKline(**kline).publish()
        if self._kline_update_callback:
            SingleTask.run(self._kline_update_callback, kline)
        if self.islog:
            logger.info("symbol:", symbol, "kline:", kline, caller=self)
    
    #加修饰器使得行情信息依次处理,如果之前的数据未处理完新数据直接抛弃，避免数据堆积新旧穿插
    #@async_method_locker("OkxMarket_process_tickers",wait=False)
    async def process_tickers(self, symbol, data,msg):
        """Process aggTrade data and publish TradeEvent."""
        data = data[0]
        ticker = {
            "Info":msg,
            "platform": self._platform,
            "symbol": symbol,
            "Sellamount"     : data.get("askSz"),              
            "Sell"    : data.get("askPx"),              
            "Buy"     : data.get("bidPx"),              
            "Buyamount"    : data.get("bidSz"),             
            "Time"    : int(data.get("ts"))   
        }
        if self.ispublic_to_mq:
            EventTrade(**ticker).publish()
        if self._tickers_update_callback:
            SingleTask.run(self._tickers_update_callback, ticker)
        if self.islog:
            logger.info("symbol:", symbol, "ticker:", ticker, caller=self)
    
    async def process_bookone(self, symbol, data,msg):
        """Process aggTrade data and publish TradeEvent."""
        data = data[0]
        ticker = {
            "Info":msg,
            "platform": self._platform,
            "symbol": symbol,
            "Sellamount"     : data.get("asks",[])[0][1],              
            "Sell"    : data.get("asks",[])[0][0],              
            "Buy"     : data.get("bids",[])[0][0],              
            "Buyamount"    : data.get("bids",[])[0][1],             
            "Time"    : int(data.get("ts"))   
        }
        if self.ispublic_to_mq:
            EventTrade(**ticker).publish()
        if self._tickers_update_callback:
            SingleTask.run(self._tickers_update_callback, ticker)
        if self.islog:
            logger.info("symbol:", symbol, "ticker:", ticker, caller=self)
    
    #加修饰器使得行情信息依次处理,如果之前的数据未处理完新数据直接抛弃，避免数据堆积新旧穿插
    #@async_method_locker("OkxMarket_process_orderbook",wait=False)
    async def process_orderbook(self,symbol, data,msg):
        """ Deal with orderbook message that updated.

        Args:
            data: Newest orderbook data.
        """
        bids = []
        asks = []
        for bid in data[0].get("bids")[:self._orderbook_length]:
            bids.append({"Price":bid[0],"Amount":bid[1]})
        for ask in data[0].get("asks")[:self._orderbook_length]:
            asks.append({"Price":ask[0],"Amount":ask[1]})
        orderbook = {
            "Info":msg,
            "platform": self._platform,
            "symbol": symbol,
            "Asks": asks,
            "Bids": bids,
            "Time": int(data[0].get("ts"))
        }
        if self._orderbook_update_callback:
            SingleTask.run(self._orderbook_update_callback, orderbook)
        if self.ispublic_to_mq:
            EventOrderbook(**orderbook).publish()
        if self.islog:
            logger.info("symbol:", symbol, "orderbook:", orderbook, caller=self)
    
    
