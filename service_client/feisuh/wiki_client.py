import lark_oapi as lark
from lark_oapi.api.wiki.v2 import *
from common_sdk.system.sys_env import get_env
from common_sdk.logging.logger import logger
import backoff

class RetryException(Exception):
    """自定义异常类用于处理需要重试的情况"""
    pass

class WikiClient:
    
    def __init__(self, enable_token: bool = False, log_level: lark.LogLevel = lark.LogLevel.INFO):
        self.app_id = get_env("WIKI_APP_ID")
        self.app_secret = get_env("WIKI_APP_SECRET")
        self.enable_token = enable_token
        self.log_level = log_level
        self.client = self._create_client()

    def _create_client(self):
        builder = lark.Client.builder().app_id(self.app_id).app_secret(self.app_secret)
        if self.enable_token:
            builder.enable_set_token(True)
        builder.log_level(self.log_level)
        return builder.build()

    @backoff.on_exception(backoff.expo, RetryException, max_tries=3)
    async def aget_node_space(self, token: str):
        """
        获取节点空间（异步）
        :param token: 节点空间令牌
        :return: 获取结果
        """
        request = GetNodeSpaceRequest.builder().token(token).build()
        response: GetNodeSpaceResponse = await self.client.wiki.v2.space.aget_node(request)
        return self._process_response(response)

    @backoff.on_exception(backoff.expo, RetryException, max_tries=3)
    def get_node_space(self, token: str):
        """
        获取节点空间（同步）
        :param token: 节点空间令牌
        :return: 获取结果
        """
        request = GetNodeSpaceRequest.builder().token(token).build()
        response: GetNodeSpaceResponse = self.client.wiki.v2.space.get_node(request)
        return self._process_response(response)

    def _process_response(self, response):
        if response.code == 1254607:
            raise RetryException(f"Request failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}")
        if not response.success():
            lark.logger.error(
                f"Request failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}")
            return None
        lark.logger.info(lark.JSON.marshal(response.data, indent=4))
        return response.data