# -*- coding:utf-8 -*-

"""
Asynchronous HTTP Request Client.

Author: xunfeng
Date:   2018/05/03
Email:  xunfeng@test.com
"""

import json
import aiohttp
from urllib.parse import urlparse

from quant.utils import logger
from quant.config import config

import requests

class AsyncHttpRequests(object):
    """ Asynchronous HTTP Request Client.
    """

    # Every domain name holds a connection session, for less system resource utilization and faster request speed.
    _SESSIONS = {}  # {"domain-name": session, ... }

    @classmethod
    async def fetch(cls, method, url, params=None, body=None, data=None, headers=None, timeout=30,logrequestinfo=False, **kwargs):
        """ Create a HTTP request.

        Args:
            method: HTTP request method. (GET/POST/PUT/DELETE)
            url: Request url.
            params: HTTP query params.
            body: HTTP request body, string or bytes format.
            data: HTTP request body, dict format.
            headers: HTTP request header.
            timeout: HTTP request timeout(seconds), default is 30s.

            kwargs:
                proxy: HTTP proxy.

        Return:
            code: HTTP response code.
            success: HTTP response data. If something wrong, this field is None.
            error: If something wrong, this field will holding a Error information, otherwise it's None.

        Raises:
            HTTP request exceptions or response data parse exceptions. All the exceptions will be captured and return
            Error information.
        """
        session = cls._get_session(url)
        if not kwargs.get("proxy"):
            kwargs["proxy"] = config.proxy  # If there is a HTTP PROXY specific in config file?
        try:
            if method == "GET":
                response = await session.get(url, params=params, headers=headers, timeout=timeout, **kwargs)
            elif method == "POST":
                response = await session.post(url, params=params, data=body, json=data, headers=headers,
                                              timeout=timeout, **kwargs)
            elif method == "PUT":
                response = await session.put(url, params=params, data=body, json=data, headers=headers,
                                             timeout=timeout, **kwargs)
            elif method == "DELETE":
                response = await session.delete(url, params=params, data=body, json=data, headers=headers,
                                                timeout=timeout, **kwargs)
            else:
                error = "http method error!"
                return None, None, error
        except Exception as e:
            logger.error("method:", method, "url:", url, "headers:", headers, "params:", params, "body:", body,
                         "data:", data, "Error:", e, caller=cls)
            return None, None, e
        if logrequestinfo:
            logger.info("method:", method, "url:", url, "headers:", headers, "params:", params, "body:", body,
                         "data:", data, caller=cls)
        code = response.status
        if code not in (200, 201, 202, 203, 204, 205, 206):
            text = await response.text()
            logger.error("method:", method, "url:", url, "headers:", headers, "params:", params, "body:", body,
                         "data:", data, "code:", code, "result:", text, caller=cls)
            return response.headers, None, text
        try:
            result = await response.json()
        except:
            result = await response.text()
            logger.warn("response data is not json format!", "method:", method, "url:", url, "headers:", headers,
                        "params:", params, "body:", body, "data:", data, "code:", code, "result:", result, caller=cls)
        logger.debug("method:", method, "url:", url, "headers:", headers, "params:", params, "body:", body,
                     "data:", data, "code:", code, "result:", json.dumps(result), caller=cls)
        return response.headers, result, None

    @classmethod
    async def get(cls, url, params=None, body=None, data=None, headers=None, timeout=30, **kwargs):
        """ HTTP GET
        """
        result = await cls.fetch("GET", url, params, body, data, headers, timeout, **kwargs)
        return result

    @classmethod
    async def post(cls, url, params=None, body=None, data=None, headers=None, timeout=30, **kwargs):
        """ HTTP POST
        """
        result = await cls.fetch("POST", url, params, body, data, headers, timeout, **kwargs)
        return result

    @classmethod
    async def delete(cls, url, params=None, body=None, data=None, headers=None, timeout=30, **kwargs):
        """ HTTP DELETE
        """
        result = await cls.fetch("DELETE", url, params, body, data, headers, timeout, **kwargs)
        return result

    @classmethod
    async def put(cls, url, params=None, body=None, data=None, headers=None, timeout=30, **kwargs):
        """ HTTP PUT
        """
        result = await cls.fetch("PUT", url, params, body, data, headers, timeout, **kwargs)
        return result

    @classmethod
    def _get_session(cls, url):
        """ Get the connection session for url's domain, if no session, create a new.

        Args:
            url: HTTP request url.

        Returns:
            session: HTTP request session.
        """
        parsed_url = urlparse(url)
        key = parsed_url.netloc or parsed_url.hostname
        if key not in cls._SESSIONS:
            session = aiohttp.ClientSession()
            cls._SESSIONS[key] = session
        return cls._SESSIONS[key]


class SyncHttpRequests(object):
    """ synchronous HTTP Request Client.
    """

    # Every domain name holds a connection session, for less system resource utilization and faster request speed.
    _SESSIONS = {}  # {"domain-name": session, ... }

    @classmethod
    def fetch(self, method, url, params=None, body=None, data=None, headers=None, timeout=30,logrequestinfo=False, **kwargs):
        """ Create a HTTP request.

        Args:
            method: HTTP request method. (GET/POST/PUT/DELETE)
            url: Request url.
            params: HTTP query params.
            body: HTTP request body, string or bytes format.
            data: HTTP request body, dict format.
            headers: HTTP request header.
            timeout: HTTP request timeout(seconds), default is 30s.

            kwargs:
                proxy: HTTP proxy.

        Return:
            code: HTTP response code.
            success: HTTP response data. If something wrong, this field is None.
            error: If something wrong, this field will holding a Error information, otherwise it's None.

        Raises:
            HTTP request exceptions or response data parse exceptions. All the exceptions will be captured and return
            Error information.
        """
        session = self.get_session(url)
        if not kwargs.get("proxies"):
            kwargs["proxies"] = {"http":config.proxy,"https":config.proxy}  # If there is a HTTP PROXY specific in config file?
        try:
            if method == "GET":
                response = session.get(url, params=params, headers=headers, timeout=timeout, **kwargs)
            elif method == "POST":
                response = session.post(url, params=params, data=body, json=data, headers=headers,
                                              timeout=timeout, **kwargs)
            elif method == "PUT":
                response = session.put(url, params=params, data=body, json=data, headers=headers,
                                             timeout=timeout, **kwargs)
            elif method == "DELETE":
                response = session.delete(url, params=params, data=body, json=data, headers=headers,
                                                timeout=timeout, **kwargs)
            else:
                error = "http method error!"
                return None, None, error
        except Exception as e:
            logger.error("method:", method, "url:", url, "headers:", headers, "params:", params, "body:", body,
                         "data:", data, "Error:", e, caller=self)
            return None, None, e
        if logrequestinfo:
            logger.info("method:", method, "url:", url, "headers:", headers, "params:", params, "body:", body,
                         "data:", data, caller=self)
        code = response.status_code
        if code not in (200, 201, 202, 203, 204, 205, 206):
            text = response.text
            logger.error("method:", method, "url:", url, "headers:", headers, "params:", params, "body:", body,
                         "data:", data, "code:", code, "result:", text, caller=self)
            return response.headers, None, text
        try:
            result = response.json()
        except:
            result = response.text
            logger.warn("response data is not json format!", "method:", method, "url:", url, "headers:", headers,
                        "params:", params, "body:", body, "data:", data, "code:", code, "result:", result, caller=self)
        logger.debug("method:", method, "url:", url, "headers:", headers, "params:", params, "body:", body,
                     "data:", data, "code:", code, "result:", json.dumps(result), caller=self)
        return response.headers, result, None

    @classmethod
    def get(self, url, params=None, body=None, data=None, headers=None, timeout=30, **kwargs):
        """ HTTP GET
        """
        result = self.fetch("GET", url, params, body, data, headers, timeout, **kwargs)
        return result

    @classmethod
    def post(self, url, params=None, body=None, data=None, headers=None, timeout=30, **kwargs):
        """ HTTP POST
        """
        result = self.fetch("POST", url, params, body, data, headers, timeout, **kwargs)
        return result

    @classmethod
    def delete(self, url, params=None, body=None, data=None, headers=None, timeout=30, **kwargs):
        """ HTTP DELETE
        """
        result = self.fetch("DELETE", url, params, body, data, headers, timeout, **kwargs)
        return result

    @classmethod
    async def put(self, url, params=None, body=None, data=None, headers=None, timeout=30, **kwargs):
        """ HTTP PUT
        """
        result = self.fetch("PUT", url, params, body, data, headers, timeout, **kwargs)
        return result

    @classmethod
    def get_session(self, url):
        """ Get the connection session for url's domain, if no session, create a new.

        Args:
            url: HTTP request url.

        Returns:
            session: HTTP request session.
        """
        parsed_url = urlparse(url)
        key = parsed_url.netloc or parsed_url.hostname
        if key not in self._SESSIONS:
            session = requests.Session()
            self._SESSIONS[key] = session
        return self._SESSIONS[key]
