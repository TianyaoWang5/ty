# -*- coding:utf-8 -*-

"""
Market Server.

Market Server will get market data from Exchange via Websocket or REST as soon as possible, then packet market data into
MarketEvent and publish into EventCenter.

Author: HuangTao
Date:   2018/05/04
Email:  huangtao@ifclover.com
"""

import sys

from quant import const
from quant.quant import quant
from quant.config import config
from quant.platform import binance_future
from quant.utils import logger
from quant.market import Orderbook
from quant.market import Kline
import time
from quant.utils import tools

from quant.utils.decorator import async_method_locker


def initialize():
    """Initialize Server."""

    from test import Tymain
    Tymain()


def main():
    config_file = sys.argv[1]  # config file, e.g. config.json.
    quant.initialize(config_file)
    initialize()
    quant.start()


if __name__ == "__main__":
    main()
