# -*- coding:utf-8 -*-

"""
Asynchronous driven quantitative trading framework.

Author: xunfeng
Date:   2022/04/12
Email:  xunfeng@test.com
"""

import signal
import asyncio
import requests
import json
from quant.utils import logger
from quant.config import config


class Quant:
    """ Asynchronous driven quantitative trading framework.
    """

    def __init__(self):
        self.loop = None
        self.event_center = None

    def initialize(self, config_module=None,configsrc = "file"):
        """ Initialize.

        Args:
            config_module: config file path, normally it"s a json file.或者是json文件
            configsrc: 配置源默认为文件,也可以是json对象
        """
        self._get_event_loop()
        self._load_settings(config_module,configsrc=configsrc)
        self._init_logger()
        self._init_db_instance()
        self._init_event_center()
        self._do_heartbeat()

    def start(self):
        """Start the event loop."""
        def keyboard_interrupt(s, f):
            print("KeyboardInterrupt (ID: {}) has been caught. Cleaning up...".format(s))
            self.loop.stop()
        signal.signal(signal.SIGINT, keyboard_interrupt)

        logger.info("start io loop ...", caller=self)
        self.loop.run_forever()

    def stop(self,username=None,robotid=None):
        """Stop the event loop."""
        logger.info("stop io loop.", caller=self)
        try:
            if username and robotid:                
                ipaddress = config.tyquantcentreserver.get("host", "120.26.56.32")
                port = config.tyquantcentreserver.get("port", "8088")
                url = "http://"+ipaddress+":"+port+"/remoteupdaterobotstatus/"
                data = {
                    "username":username,
                    "robotid":robotid,
                    "statuscode":3
                }
                headers = {
                    'Connection': 'close',
                }
                trynum = 0
                while trynum<5:
                    try:
                        if port=="8088":
                            res = requests.post(url=url,data=data,headers=headers,timeout=10)
                        else:
                            res = requests.post(url=url,json=data,headers=headers,timeout=10)
                        resjson = json.loads(res.text)
                        if resjson["code"]==0:
                            break
                        logger.info(str(json.loads(res.text)), caller=self)
                    except Exception as r:
                        logger.info("更新机器人状态出错"+str(trynum)+str(r), caller=self)
                    trynum += 1
            else:
                logger.info("未输入用户名与机器人ID", caller=self)
        except Exception as r:
            logger.info("更新机器人状态出错"+str(r), caller=self)
        self.loop.stop()

    def _get_event_loop(self):
        """ Get a main io loop. """
        if not self.loop:
            self.loop = asyncio.get_event_loop()
        return self.loop

    def _load_settings(self, config_module,configsrc="file"):
        """ Load config settings.

        Args:
            config_module: config file path, normally it"s a json file.
        """
        config.loads(config_module,configsrc=configsrc)

    def _init_logger(self):
        """Initialize logger."""
        console = config.log.get("console", True)
        level = config.log.get("level", "DEBUG")
        path = config.log.get("path", "/tmp/logs/Quant")
        name = config.log.get("name", "quant.log")
        clear = config.log.get("clear", False)
        backup_count = config.log.get("backup_count", 0)
        if console:
            logger.initLogger(level)
        else:
            logger.initLogger(level, path, name, clear, backup_count)

    def _init_db_instance(self):
        """Initialize db."""
        if config.mongodb:
            from quant.utils.mongo import initMongodb
            initMongodb(**config.mongodb)

    def _init_event_center(self):
        """Initialize event center."""
        if config.rabbitmq:
            from quant.event import EventCenter
            self.event_center = EventCenter()
            self.loop.run_until_complete(self.event_center.connect())
            config.register_run_time_update()

    def _do_heartbeat(self):
        """Start server heartbeat."""
        from quant.heartbeat import heartbeat
        self.loop.call_later(0.5, heartbeat.ticker)


quant = Quant()
