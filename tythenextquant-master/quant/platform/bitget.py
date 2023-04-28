# -*- coding:utf-8 -*-

"""
Bitget Trade module.
https://www.bitget.me/docs/zh/

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
from urllib.parse import urljoin
import math
import traceback

from quant.error import Error
from quant.utils import tools
from quant.utils import logger
from quant.const import BITGET
from quant.order import Order
from quant.tasks import SingleTask,LoopRunTask
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

from quant import const
from quant.utils.web import Websocket
from quant.order import ORDER_ACTION_BUY, ORDER_ACTION_SELL
from quant.event import EventTrade, EventKline, EventOrderbook

__all__ = ("BitgetRestAPI", "BitgetTrade", )


class BitgetRestAPI:
    """ Bitget REST API client.
    实盘API交易地址如下：

    REST：https://api.bitget.com
    WebSocket公共频道：wss://ws.bitget.com/spot/v1/stream
    # WebSocket私有频道：wss://ws.bitget.com:8443/ws/v5/private
    # AWS 地址如下：

    # REST：https://aws.bitget.com
    # WebSocket公共频道：wss://wsaws.bitget.com:8443/ws/v5/public
    # WebSocket私有频道：wss://wsaws.bitget.com:8443/ws/v5/private

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
            exsymbol = symbol.replace('_',"")
        else:
            success = {}
            error = "请输入要获取交易规则的交易对"
            logger.info(error,caller=self)
            return success, error
        successres=None
        while True:
            try:
                exchangeInfo, error = await self.request("GET", "/api/spot/v1/public/products", "")
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
                        if info["symbolName"]==exsymbol:
                            minQty=float(info["minTradeAmount"])
                            maxQty=float(info["maxTradeAmount"])
                            amountSize = int(info["quantityScale"])
                            priceSize=int(info["priceScale"])
                            tickSize = float(10**(-1*int(info["priceScale"])))
                            minNotional = float(info["minTradeUSDT"]) #名义价值
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

    async def GetTicker(self, symbol=None, autotry=False, sleep=100):
        """ 获取当前交易对、合约对应的市场当前行情，返回值:Ticker结构体。

        symbol:BTCUSDT

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if symbol:
            symbol = symbol.replace('_', "")+"_SPBL"
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
                success, error = await self.request("GET", "/api/spot/v1/market/ticker", params)
                successres = success
                if success:
                    success={
                        "Info"    : success,             #请求交易所接口后，交易所接口应答的原始数据，回测时无此属性
                        "High"    : float(success["data"]["high24h"]),              #最高价，如果交易所接口没有提供24小时最高价则使用卖一价格填充
                        "Low"     : float(success["data"]["low24h"]),               #最低价，如果交易所接口没有提供24小时最低价则使用买一价格填充
                        "Sellamount": float(success["data"]["askSz"]),
                        "Sell"    : float(success["data"]["sellOne"]),               #卖一价
                        "Buy"     : float(success["data"]["buyOne"]),               #买一价
                        "Buyamount": float(success["data"]["bidSz"]), 
                        "Last"    : float(success["data"]["close"]),               #最后成交价
                        "Volume"  : float(success["data"]["baseVol"]),          #最近24h成交量(基础币)
                        "Time"    : int(success["data"]["ts"])      #毫秒级别时间戳
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
                error = e
                if not autotry:
                    logger.info("Ticker行情更新报错",successres,error,e,caller=self)
                    break
                else:
                    logger.info("Ticker行情更新报错，休眠",sleep,"毫秒后重试",successres,error,caller=self)
                    await tools.Sleep(sleep/1000)
        return success, error

    async def GetDepth(self, symbol=None,limit=150, autotry= False, sleep=100):
        """ 获取当前交易对、合约对应的市场的订单薄数据，返回值：Depth结构体。

        symbol:BTC-USDT

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        # ts = tools.get_cur_timestamp_ms()
        if symbol:
            symbol = symbol.replace('_', "")+"_SPBL"
            params = {
                "symbol":symbol,
                "type":"step0",
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
                success, error = await self.request("GET", "/api/spot/v1/market/depth", params)
                successres = success
                if success:
                    Bids=[]
                    Asks=[]
                    for dp in success["data"]["bids"]:
                        Bids.append({"Price":float(dp[0]),"Amount":float(dp[1])})
                    for dp in success["data"]["asks"]:
                        Asks.append({"Price":float(dp[0]),"Amount":float(dp[1])})
                    success={
                        "Info"    : success,
                        "Asks"    : Asks,             #卖单数组，MarketOrder数组,按价格从低向高排序
                        "Bids"    : Bids,             #买单数组，MarketOrder数组,按价格从高向低排序
                        "Time"    : int(success["data"]["timestamp"])      #毫秒级别时间戳
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
    
    async def GetTrades(self, symbol=None,limit=100, autotry=False, sleep=100):
        """ 获取当前交易对、合约对应的市场的交易历史（非自己），返回值：Trade结构体数组
        # 未归集数据
        symbol:BTC-USDT

        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        if symbol:
            symbol = symbol.replace('_', "")+"_SPBL"
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
                success, error = await self.request("GET", "/api/spot/v1/market/fills", params)
                successres = success
                if success:
                    aggTrades = []
                    for tr in success["data"]:
                        aggTrades.append({
                            "Id":int(tr["tradeId"]),
                            "Time":int(tr["fillTime"]),
                            "Price":float(tr["fillPrice"]),
                            "Amount":float(tr["fillQuantity"]),
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
                error = e
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
                    success, error = await self.request("GET", "/api/spot/v1/account/assets", params, auth=True)
                    successres = success
                    if success:
                        Stocks = 0
                        FrozenStocks = 0
                        Balance = 0
                        FrozenBalance = 0
                        for ac in success["data"]:
                            if ac["coinName"].upper() == basesymbol:
                                Stocks = float(ac["available"])
                                FrozenStocks = float(ac["frozen"])
                            if ac["coinName"].upper() == quotesymbol:
                                Balance = float(ac["available"])
                                FrozenBalance = float(ac["frozen"])
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
                    error = e
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
                "symbol": "BTCUSDT_SPBL",
                "side": "buy",
                "orderType": "limit",
                "force": "normal",
                "price": "23222.5",
                "quantity": "1",
                "clientOrderId": "myorder_16569403333"
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
        ---Bitget---,
        类型	            强制要求的参数
        limit	            未知
        market	            未知
            orderType,
                limit 限价单
                market 市价单
            force:
                normal:不用特殊控制类型订单,
                post_only:postOnly类型订单,
                fok:全部成交或立即取消,
                ioc	立即成交并取消剩余
        """
        if ttype == "LIMIT" and timeInForce == "GTC":   # 常用
            orderType = "limit"
            force = "normal"
        elif ttype == "LIMIT" and timeInForce == "IOC":   # 常用
            orderType = "limit"
            force = "ioc"
        elif ttype == "LIMIT" and timeInForce == "FOK":   # 常用
            orderType = "limit"
            force = "fok"
        elif ttype == "MARKET":
            orderType = "market"
            force = "normal"
        elif ttype == "LIMIT_MAKER":   # 常用
            orderType = "limit"
            force = "post_only"
        else:
            orderType = "no type"
        symbol = symbol.replace('_', "")+"_SPBL"
        if orderType != "no type":
            info = {
                "symbol": symbol,
                "side": "buy",
                "orderType": orderType,
                "force": force,
                "quantity": quantity, # 精度问题，科学计数法问题
                "price": price, # 精度问题，科学计数法问题
            }
        else:
            logger.info("参数orderType错误,orderType:",orderType,caller=self)
            error = "参数orderType错误!"
            return None, error
        success, error = await self.request("POST", "/api/spot/v1/trade/orders", body=info, auth=True,logrequestinfo=logrequestinfo)
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
            orderType = "limit"
            force = "normal"
        elif ttype == "LIMIT" and timeInForce == "IOC":   # 常用
            orderType = "limit"
            force = "ioc"
        elif ttype == "LIMIT" and timeInForce == "FOK":   # 常用
            orderType = "limit"
            force = "fok"
        elif ttype == "MARKET":
            orderType = "market"
            force = "normal"
        elif ttype == "LIMIT_MAKER":   # 常用
            orderType = "limit"
            force = "post_only"
        else:
            orderType = "no type"
        symbol = symbol.replace('_', "")+"_SPBL"
        if orderType != "no type":
            info = {
                "symbol": symbol,
                "side": "sell",
                "orderType": orderType,
                "force": force,
                "quantity": quantity, # 精度问题，科学计数法问题
                "price": price, # 精度问题，科学计数法问题
            }
        else:
            logger.info("参数orderType错误,orderType:",orderType,caller=self)
            error = "参数orderType错误!"
            return None, error
        success, error = await self.request("POST", "/api/spot/v1/trade/orders", body=info, auth=True,logrequestinfo=logrequestinfo)
        return success, error

    async def CancelOrder(self, symbol, order_id = None, client_order_id = None):
        """ Cancelling an unfilled order.
        Args:
            symbol: Symbol name, e.g. BTCUSDT.
            order_id: Order id.
            client_order_id: Client order id.
            当传入2种ID时,以order_id为准
            当没有传入订单ID时,撤销单一交易对的所有订单
            {
                "symbol": "BTCUSDT_SPBL",
                "orderId": "34923828882"
            }
            {
                "symbol": "BTCUSDT_SPBL",
                "orderIds": [
                    "34923828882"
                    ]
            }
        Returns:
            success: Success results, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        symbol = symbol.replace('_',"")+"_SPBL"
        if order_id:
            bodys = {
                "symbol": symbol,
                "orderId": str(order_id),
            }
            success, error = await self.request("POST", "/api/spot/v1/trade/cancel-order", body=bodys, auth=True)
        else:
            bodys_pending = {
                "symbol": symbol,
            }
            # 获取未成交订单
            success_orders, error = await self.request("POST", "/api/spot/v1/trade/open-orders", body=bodys_pending, auth=True)
            if error:
                return success_orders, error
            bodys={"symbol": symbol,"orderIds": []}
            for row in success_orders["data"]:
                if(row["symbol"] == symbol):
                    bodys["orderIds"].append(row["orderId"])
            success, error = await self.request("POST", "/api/spot/v1/trade/cancel-batch-orders", body=bodys, auth=True)
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
            symbol = symbol.replace('_',"")+"_SPBL"
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
            bodys = {
                "symbol": symbol,
                "orderId": str(order_id),
            }
            if client_order_id:
                bodys = {
                    "symbol": symbol,
                    "origClientOrderId": str(client_order_id),
                }
            success, error = await self.request("POST", "/api/spot/v1/trade/orderInfo", body=bodys, auth=True,logrequestinfo=logrequestinfo)
            successres = success   
            if success:       
                if success.get("data",[])[0]:
                    success=success.get("data",[])[0]
                    if success["status"]=="cancelled":
                        status="已取消"
                    elif success["status"]=="full_fill":
                        status="已完成"
                    elif success["status"]=="partial_fill" or success["status"]=="new":
                        status="未完成"
                    else:
                        status="其他"    
                    try:
                        Price = float(success["price"])
                    except ValueError:
                        Price = 0.0    
                    try:
                        Amount = float(success["quantity"])
                    except ValueError:
                        Amount = 0.0 
                    try:
                        DealAmount = float(success["fillQuantity"])
                    except ValueError:
                        DealAmount = 0.0 
                    try:
                        AvgPrice = 0 if float(success["fillQuantity"])==0 else float(success["fillTotalAmount"])/float(success["fillQuantity"])
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
                        "Type"        : "Buy" if success["side"]=="buy" else "Sell",             # 订单类型，参考常量里的订单类型，例如：ORDER_TYPE_BUY
                        "Offset"      : 0,             # 数字货币期货的订单数据中订单的开平仓方向。ORDER_OFFSET_OPEN为开仓方向，ORDER_OFFSET_CLOSE为平仓方向
                        "ContractType" : ""            # 现货订单中该属性为""即空字符串，期货订单该属性为具体的合约代码
                    }
                else:
                    orderinfo={}    
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
            headers["ACCESS-KEY"] = self._access_key.encode().decode()
            headers["ACCESS-SIGN"] = sign.decode()
            headers["ACCESS-TIMESTAMP"] = str(timestamp)
            headers["ACCESS-PASSPHRASE"] = self._passphrase
            headers["LOCALE"] = 'zh-CN'
        _header, success, error = await AsyncHttpRequests.fetch(method, url, body=body, headers=headers, timeout=10,logrequestinfo=logrequestinfo)
        if rethead:
            return success, error, _header
        else:
            return success, error

    # def sign(message, secret_key):
    #     mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
    #     d = mac.digest()
    #     return base64.b64encode(d)

    # def pre_hash(timestamp, method, request_path, body):
    #     return str(timestamp) + str.upper(method) + request_path + body

    # if __name__ == '__main__':
    #     signStr = sign(
    #         pre_hash('1659927638003', 'POST', '/api/spot/v1/trade/orders', str('{"symbol":"TRXUSDT_SPBL","side":"buy","orderType":"limit","force":"normal","price":"0.046317","quantity":"1212"}')
    #         ), '')
    # print(signStr)


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
        #     headers["ACCESS-KEY"] = self._access_key.encode().decode()
        #     headers["ACCESS-SIGN"] = sign.decode()
        #     headers["ACCESS-TIMESTAMP"] = str(timestamp)
        #     headers["ACCESS-PASSPHRASE"] = self._passphrase
        # headers["locale"] = "zh-CN"
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
            headers["ACCESS-KEY"] = self._access_key.encode().decode()
            headers["ACCESS-SIGN"] = sign.decode()
            headers["ACCESS-TIMESTAMP"] = str(timestamp)
            headers["ACCESS-PASSPHRASE"] = self._passphrase
            headers["locale"] = "zh-CN"
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
        #     headers["ACCESS-KEY"] = self._access_key.encode().decode()
        #     headers["ACCESS-SIGN"] = sign.decode()
        #     headers["ACCESS-TIMESTAMP"] = str(timestamp)
        #     headers["ACCESS-PASSPHRASE"] = self._passphrase
        # headers["locale"] = "zh-CN"
        _header, success, error = SyncHttpRequests.fetch(method, url, headers=headers, timeout=10, verify=False,logrequestinfo=logrequestinfo)
        if rethead:
            return success, error, _header
        else:
            return success, error

class BitgetTrade(Websocket):
    """ Bitget Trade module. You can initialize trade object with some attributes in kwargs.

    Attributes:
        account: Account name for this trade exchange.
        strategy: What's name would you want to created for you strategy.
        symbol: Symbol name for your trade.
        host: HTTP request host. (default "https://www.bitget.com")
        wss: Websocket address. (default "wss://real.bitget.com:8443")
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
            kwargs["host"] = "https://www.bitget.com"
        if not kwargs.get("wss"):
            kwargs["wss"] = "wss://real.bitget.com:8443"
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
        self._platform = BITGET
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
        super(BitgetTrade, self).__init__(url, send_hb_interval=5)
        self.heartbeat_msg = "ping"

        self._assets = {}  # Asset object. e.g. {"BTC": {"free": "1.1", "locked": "2.2", "total": "3.3"}, ... }
        self._orders = {}  # Order objects. e.g. {"order_no": Order, ... }

        # Initializing our REST API client.
        self._rest_api = BitgetRestAPI(self._host, self._access_key, self._secret_key, self._passphrase)

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

    @async_method_locker("BitgetTrade.process_binary.locker")
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

#交易所行情数据对象
#通过ws接口获取行情，并通过回调函数传递给策略使用
class BitgetMarket:
    """ Bitget Market Server.

    Attributes:
        ispublic_to_mq:是否将行情推送到行情中心,默认为否
        islog:是否logger输出行情数据,默认为否
        orderbook_update_callback:订单薄数据回调函数
        kline_update_callback:K线数据回调函数
        trade_update_callback:成交数据回调函数
        kwargs:
            platform: Exchange platform name, must be `Bitget`.
            wss: Exchange Websocket host address, default is `wss://ws.bitget.com/spot/v1/stream`.
            symbols: Symbol list.
            channels: Channel list, only `orderbook` / `trade` / `kline` to be enabled.
            orderbook_length: The length of orderbook's data to be published via OrderbookEvent, default is 10.
    """

    def __init__(self,ispublic_to_mq=False,islog=False, orderbook_update_callback=None,kline_update_callback=None,trade_update_callback=None,tickers_update_callback=None, **kwargs):
        self.ispublic_to_mq=ispublic_to_mq
        self.islog=islog
        self._platform = kwargs.get("platform")
        self._account = kwargs.get("account")

        self._wss = kwargs.get("wss", "wss://ws.bitget.com/spot/v1/stream")
        self._symbols = list(set(kwargs.get("symbols")))
        self._channels = kwargs.get("channels")
        self._orderbook_length = 15

        self._c_to_s = {}
        self._tickers = {}
        self.isalive = False


        url = self._make_url()
        self._ws = Websocket(url, connected_callback=self.connected_callback,process_callback=self.BitgetMarket_process)
        self._ws.initialize()
        LoopRunTask.register(self.send_heartbeat_msg, 15)
        
       
        kwargs["orderbook_update_callback"] = self.process_orderbook
        kwargs["kline_update_callback"] = self.process_kline
        kwargs["trade_update_callback"] = self.process_trade
        kwargs["tickers_update_callback"] = self.process_tickers
        self._orderbook_update_callback = orderbook_update_callback
        self._kline_update_callback = kline_update_callback
        self._trade_update_callback = trade_update_callback
        self._tickers_update_callback = tickers_update_callback
    
    def find_closest(self,num):
        arr = [5, 15]
        closest = arr[0]
        for val in arr:
            if abs(val - num) < abs(closest - num):
                closest = val
        return closest
    
    async def send_heartbeat_msg(self, *args, **kwargs):
        d = "ping"
        if not self._ws:
            self.isalive = False
            logger.error("Websocket connection not yeah!", caller=self)
            return
        await self._ws.send(d)

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
                    args.append({
                        "instType": "sp",
                        "channel": "candle1m",
                        "instId": symbol
                    })
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
                    args.append({
                            "instType": "sp",
                            "channel": "books"+str(self.find_closest(self._orderbook_length)),
                            "instId": symbol
                        })
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
                    args.append({
                        "instType": "sp",
                        "channel": "trade",
                        "instId": symbol
                    })
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
                    args.append({
                        "instType": "SP",
                        "channel": "ticker",
                        "instId": symbol
                    })
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

    async def BitgetMarket_process(self, msg):
        """Process message that received from Websocket connection.

        Args:
            msg: Message received from Websocket connection.
        """
        #logger.debug("msg:", msg, caller=self)
        self.isalive = True
        if not isinstance(msg, dict):
            return
        channel = msg.get("arg",{}).get("channel","")
        #if channel not in self._channels:
        #    logger.warn("unkown channel, msg:", msg, caller=self)
        #    return

        data = msg.get("data",[])
        symbol = msg.get("arg",{}).get("instId","")
        if data:
            pass
        else:
            #logger.info("返回数据有误:", msg, caller=self)
            return
        if "candle" in channel:
            await self.process_kline(symbol, data,msg)
        elif "books" in channel:
            await self.process_orderbook(symbol, data,msg)
        elif "trade" in channel:
            await self.process_trade(symbol, data,msg)
        elif "ticker" in channel:
            await self.process_tickers(symbol, data,msg)

    #加修饰器使得行情信息依次处理,如果之前的数据未处理完新数据直接抛弃，避免数据堆积新旧穿插
    #@async_method_locker("BybitMarketv3_process_kline",wait=False)
    async def process_kline(self, symbol, data,msg):
        """Process kline data and publish KlineEvent."""
        try:
            kline = {
                "Info":msg,
                "platform": self._platform,
                "symbol": symbol,
                "Open": float(data[-1][1]),
                "High": float(data[-1][2]),
                "Low": float(data[-1][3]),
                "Close": float(data[-1][4]),
                "Volume": float(data[-1][5]),
                "Time": tools.get_cur_timestamp_ms(),
            }
            if self._kline_update_callback:
                SingleTask.run(self._kline_update_callback, kline)
            if self.ispublic_to_mq:
                EventKline(**kline).publish()
            if self.islog:
                logger.info("symbol:", symbol, "kline:", kline, caller=self)
        except:
            pass

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
        try:
            orderbook = {
                "Info":msg,
                "platform": self._platform,
                "symbol": symbol,
                "Asks": data[0].get("asks")[:self._orderbook_length],
                "Bids": data[0].get("bids")[:self._orderbook_length],
                "Time": int(data[0].get("ts"))
            }
            if self._orderbook_update_callback:
                SingleTask.run(self._orderbook_update_callback, orderbook)
            if self.ispublic_to_mq:
                EventOrderbook(**orderbook).publish()
            if self.islog:
                logger.info("symbol:", symbol, "orderbook:", orderbook, caller=self)
        except:
            pass

    #加修饰器使得行情信息依次处理,如果之前的数据未处理完新数据直接抛弃，避免数据堆积新旧穿插
    #@async_method_locker("BybitMarketv3_process_trade",wait=False)
    async def process_trade(self, symbol, data,msg):
        """Process trade data and publish TradeEvent."""
        try:
            trade = {
                "Info":msg,
                "platform": self._platform,
                "symbol": symbol,
                "Type":  0 if data[0][3]=="buy" else 1, 
                "Price": data[0][1],
                "Amount": data[0][2],
                "Time": int(data[0][0])
            }
            if self._trade_update_callback:
                SingleTask.run(self._trade_update_callback, trade)
            if self.ispublic_to_mq:
                EventTrade(**trade).publish()
            if self.islog:
                logger.info("symbol:", symbol, "trade:", trade, caller=self)
        except:
            pass
    
    #加修饰器使得行情信息依次处理,如果之前的数据未处理完新数据直接抛弃，避免数据堆积新旧穿插
    #@async_method_locker("BybitMarketv3_process_tickers",wait=False)
    async def process_tickers(self, symbol, data,msg):
        """Process aggTrade data and publish TradeEvent."""
        try:
            ticker = {
                "Info":msg,
                "platform": self._platform,
                "symbol": symbol,
                "Sellamount"     : data[0].get("askSz"),              
                "Sell"    : data[0].get("bestAsk"),              
                "Buy"     : data[0].get("bestBid"),              
                "Buyamount"    : data[0].get("bidSz"),             
                "Time"    : int(data[0].get("ts") )  
            }
            if self._tickers_update_callback:
                SingleTask.run(self._tickers_update_callback, ticker)
            if self.ispublic_to_mq:
                EventTrade(**ticker).publish()
            if self.islog:
                logger.info("symbol:", symbol, "ticker:", ticker, caller=self)
        except:
            pass

class BitgetAccount:
    """ 
    Bitgetwebsocket账户信息推送

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
            kwargs["host"] = "https://api.bitget.com"
        if not kwargs.get("wss"):
            kwargs["wss"] = "wss://ws.bitget.com/spot/v1/stream"
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
        self._platform = BITGET
        self._symbols = kwargs["symbols"]
        self._host = kwargs["host"]
        self._wss = kwargs["wss"]
        self._access_key = kwargs["access_key"]
        self._secret_key = kwargs["secret_key"]
        self._passphrase = kwargs["passphrase"]
        self._asset_update_callback = kwargs.get("asset_update_callback")
        self._order_update_callback = kwargs.get("order_update_callback")
        self._init_success_callback = kwargs.get("init_success_callback")


        self._raw_symbols = []
        for symbol in self._symbols:
            self._raw_symbols.append(symbol.replace("_", "")+"_SPBL")
        url=self._wss
        self._ws = Websocket(url, self.connected_callback, self.process)
        self._ws.initialize()

        LoopRunTask.register(self.send_heartbeat_msg, 15)

    async def get_sign(self,timestamp):
        """ Initialize Websocket connection.
        """
        message = str(timestamp) + "GET" + "/user/verify"
        mac = hmac.new(bytes(self._secret_key, encoding="utf8"), bytes(message, encoding="utf-8"),
                        digestmod="sha256")
        d = mac.digest()
        sign = base64.b64encode(d)
        return sign.decode()
        

    async def send_heartbeat_msg(self, *args, **kwargs):
        data = "ping"
        if not self._ws:
            logger.error("")
            return
        await self._ws.send(data)

    async def connected_callback(self):
        """ After websocket connection created successfully, pull back all open order information.
        """
        logger.info("Websocket connection authorized successfully.", caller=self)
        timestamp = str(time.time()).split(".")[0]
        sign = await self.get_sign(timestamp)
        d={
            "op": "login",
            "args": [{
                "apiKey": self._access_key,
                "passphrase": self._passphrase,
                "timestamp": timestamp,
                "sign": sign
            }]
        }
        await self._ws.send(json.dumps(d))
        logger.info("send login success.",caller=self)
        await tools.Sleep(1000)
        d={
            "op": "subscribe",
            "args": [{
                "instType": "spbl",
                "channel": "account",
                "instId": "default"
            }]
        }
        await self._ws.send(json.dumps(d))
        logger.info("subscribe account success.",d, caller=self)
        args=[]
        for symbol in self._raw_symbols:
            args.append({
                        "channel": "orders",
                        "instType": "spbl",
                        "instId": symbol
                    })
        culist = tools.cut_list(args,10)
        for arg in culist:
            d={
                "op": "subscribe",
                "args": arg
            }
            await self._ws.send(json.dumps(d))
        logger.info("subscribe orders success.",d, caller=self)
        if self._init_success_callback:
            SingleTask.run(self._init_success_callback, True, None)


    @async_method_locker("BitgetAccount.process.locker")
    async def process(self, msg):
        """ Process message that received from Websocket connection.

        Args:
            msg: message received from Websocket connection.
        """
        try:
            e = msg.get("arg",{}).get("channel","")
            if e == "orders":  # Order update.
                if msg["data"][0]["instId"] not in self._raw_symbols:
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
            if e == "account":  # asset update.
                """ [
                    {
                        "a": "USDT",
                        "f": "176.81254174",
                        "l": "201.575"
                    }
                ] """
                assets = []
                for ba in msg["data"]:
                    assets.append({
                                "a":ba.get("coinName"),
                                "f":float(ba.get("available")),
                                "l":float(ba.get("frozen",0.0))
                            })
                info = {
                    "platform": self._platform,
                    "account": self._account,
                    "Info":msg,
                    "assets": assets,
                    "timestamp": tools.get_cur_timestamp_ms(),
                    "update": tools.get_cur_timestamp_ms()                
                }
                asset = info
                if self._asset_update_callback:
                    SingleTask.run(self._asset_update_callback, copy.copy(asset))
        except Exception as e:
            pass