# -*- coding: utf-8 -*-

import requests
from ..base_class.singleton import SingletonMetaThreadSafe as SingletonMetaclass
from ..util.bytes_utils import MyBytes
from ..system.sys_env import get_env
from ..logging.logger import logger
from ..system import sys_env
import time
import random
import backoff
import httpx


"""linkAI的client类"""
class LinkAIClient(metaclass=SingletonMetaclass):
    """
    LinkAIClient 用于调用linkai的应用聊天
    Args:
        metaclass: _description_. Defaults to SingletonMetaclass.
    """
    @backoff.on_exception(backoff.expo,
                      httpx.HTTPError,
                      max_tries=5,
                      jitter=backoff.random_jitter)
    async def request_with_backoff(self, url, body, headers):
        async with httpx.AsyncClient() as client:
            logger.info(f"[LINKAI] 聊天请求, url {url} json {body} headers {headers}")
            res = await client.post(url, json=body, headers=headers)
            res.raise_for_status()  # 可能触发重试的异常
            return res
    
    # todo:改成backoff重试
    def application_completions(self, query, session, app_code, retry_count=0, stream=False, model="gpt-3.5-turbo-16k"):
        """
        调用linkai的应用聊天
        
        :param query: 提示词
        :param app_code: 请求应用（支持不同应用）
        :param session: session 这里的session按照用户维度
        :return: 返回值描述
        """
        # 使用逗号分割字符串成列表
        apikey_list = get_env("LINK_API_KEY").split(",")   
        # 随机选择一个apikey
        selected_apikey = random.choice(apikey_list)
        headers = {
            'Content-Type': 'application/json; charset=UTF-8',
            'Authorization': 'Bearer '+ selected_apikey
        }
        body = {
            "app_code": app_code,
            "model": model,     # 对话模型的名称, 支持 gpt-3.5-turbo, gpt-3.5-turbo-16k, gpt-4, wenxin, xunfei
            "temperature": get_env("BOT_TEMPERATURE"),
            "top_p": get_env("BOT_TOP_P"),
            "frequency_penalty": get_env("BOT_FREQUENCY_PENALTY", 0.0),  # [-2,2]之间，该值越大则更倾向于产生不同的内容
            "presence_penalty": get_env("BOT_PRESENCE_PENALTY", 0.0),  # [-2,2]之间，该值越大则更倾向于产生不同的内容
            "stream": stream
        }
        if retry_count >= 2:
            # exit from retry 2 times
            logger.warning("[LINKAI] failed after maximum number of retry times")
            text = "提问频繁，请稍后再试" 
            if stream:
                text_bytes = b'data: {"choices": [{"index": 0, "delta": {"content": "'+ text.encode('utf-8') +b'"}, "finish_reason": null}]}\r\n\r\n'
                return MyBytes(text_bytes)
            return text
        try:
            body.update({"messages": session.messages})
            service_url = sys_env.get_env("LINK_AI_BASE_URL")
            logger.info(f"[LINKAI] 聊天请求, json {body} headers {headers}")
            res = requests.post('{}/v1/chat/completions'.format(service_url), json=body, headers=headers, timeout=58)
            logger.info(f"[LINKAI] res={res.status_code}")
            if res.status_code == 200:
                logger.info(f"[LINKAI] reply={res.content}")
                return res
            else:
                response = res.json()
                logger.error(f"[LINKAI] chat failed, status_code={res.status_code}, msg={response.get('msg')}, type={response.get('type')}")
                if res.status_code >= 500:
                    # server error, need retry
                    time.sleep(2)
                    logger.info(f"[LINKAI] do retry, times={retry_count}")
                    return self.application_completions(query, session, app_code, retry_count=retry_count + 1, stream=stream, model=model)
                text = "请求频繁请稍后再试"
                if stream:
                    text_bytes = b'data: {"choices": [{"index": 0, "delta": {"content": "'+ text.encode('utf-8') +b'"}, "finish_reason": null}]}\r\n\r\n'
                    return MyBytes(text_bytes)
                return text

        except Exception as e:
            logger.exception(e)
            # retry
            time.sleep(2)
            logger.info(f"[LINKAI] do retry, times={retry_count}")
            return self.application_completions(query, session, app_code, retry_count + 1, stream=stream, model=model)
        
    async def application_completions_async(self, messages, app_code, stream=False, model="gpt-3.5-turbo-16k"):
        apikey_list = get_env("LINK_API_KEY").split(",")   
        selected_apikey = random.choice(apikey_list)
        headers = {
            'Content-Type': 'application/json; charset=UTF-8',
            'Authorization': 'Bearer ' + selected_apikey
        }
        body = {
            "app_code": app_code,
            "model": model,     # 对话模型的名称, 支持 gpt-3.5-turbo, gpt-3.5-turbo-16k, gpt-4, wenxin, xunfei
            "temperature": get_env("BOT_TEMPERATURE"),
            "top_p": get_env("BOT_TOP_P"),
            "frequency_penalty": get_env("BOT_FREQUENCY_PENALTY", 0.0),  # [-2,2]之间，该值越大则更倾向于产生不同的内容
            "presence_penalty": get_env("BOT_PRESENCE_PENALTY", 0.0),  # [-2,2]之间，该值越大则更倾向于产生不同的内容
            "stream": stream
        }
        body.update({"messages": messages})
        service_url = sys_env.get_env("LINK_AI_BASE_URL")
        url = f'{service_url}/v1/chat/completions'

        try:
            response = await self.request_with_backoff(url, body, headers)
            return response
        except httpx.HTTPStatusError as e:
            logger.exception(e)
            raise e


    def _fecth_knowledge_search_suffix(self, response) -> str:
        try:
            if response.get("knowledge_base"):
                search_hit = response.get("knowledge_base").get("search_hit")
                first_similarity = response.get("knowledge_base").get("first_similarity")
                logger.info(f"[LINKAI] knowledge base, search_hit={search_hit}, first_similarity={first_similarity}")
        except Exception as e:
            logger.exception(e)
            return False
        return search_hit

linkai_client = LinkAIClient()