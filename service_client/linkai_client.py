# -*- coding: utf-8 -*-

import requests
from ..base_class.singleton import SingletonMetaThreadSafe as SingletonMetaclass
from ..util.bytes_utils import MyBytes
from ..system.sys_env import get_env
from ..logging.logger import logger
from ..system import sys_env
from service.errors import error_codes, Error
import time
import random


"""linkAI的client类"""
class LinkAIClient(metaclass=SingletonMetaclass):

    def application_completions(self, query, session, app_code, retry_count=0, stream=False):
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
            "model": get_env("BOT_MODEL"),     # 对话模型的名称, 支持 gpt-3.5-turbo, gpt-3.5-turbo-16k, gpt-4, wenxin, xunfei
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
            res = requests.post('{}/v1/chat/completions'.format(service_url), json=body, headers=headers, timeout=180)
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
                    return self.application_completions(query, session, app_code, retry_count=retry_count + 1, stream=stream)
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
            return self.application_completions(query, session, app_code, retry_count + 1, stream=stream)


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