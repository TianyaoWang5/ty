# -*- coding:utf-8 -*-

from distutils.core import setup


setup(
    name="tythenextquant",
    version="0.2.69",
    packages=[
        "quant",
        "quant.utils",
        "quant.platform",
    ],
    description="Asynchronous driven quantitative trading framework.",
    url="",
    author="xunfeng",
    author_email="test@test.com",
    license="MIT",
    keywords=[
        "thenextquant", "quant", "framework", "async", "asynchronous", "digiccy", "digital", "currency",
        "marketmaker", "binance", "okx", "huobi", "bitmex", "deribit", "kraken", "gemini", "kucoin"
    ],
    install_requires=[
        "aiohttp>=3.2.1",
        "aioamqp>=0.13.0",
        "motor>=2.0.0",
        "psutil>=5.8.0"
    ],    
)


