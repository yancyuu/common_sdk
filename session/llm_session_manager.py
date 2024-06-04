# chatGPT的session管理器,用来存放进程中的一些全局变量，比如上下文回话
from common_sdk.auth.redis_storage import redis_storage
from common_sdk.logging.logger import logger
from common_sdk.system.sys_env import get_env
import json


class Session(object):
    """这里主要维护的是大语言模型的上下轮会话"""

    def __init__(self, session_id, system_prompt=None):
        self.session_id = session_id
        self.messages = []
        if system_prompt is None:
            self.system_prompt = get_env("BOT_CHARACTER_DESC", "")
        else:
            self.system_prompt = system_prompt
        
    def to_json(self):
        return json.dumps(self.__dict__)

    @classmethod
    def from_json(cls, json_str):
        session_dict = json.loads(json_str)
        session = cls(session_dict['session_id'], system_prompt=session_dict.get('system_prompt', None))
        session.messages = session_dict.get('messages', [])
        return session

    # 重置会话
    def reset(self):
        system_item = {"role": "system", "content": self.system_prompt}
        self.messages = [system_item]

    def set_system_prompt(self, system_prompt):
        self.system_prompt = system_prompt
        self.reset()

    def add_query(self, query):
        user_item = {"role": "user", "content": query}
        self.messages.append(user_item)

    def add_reply(self, reply):
        assistant_item = {"role": "assistant", "content": reply}
        self.messages.append(assistant_item)

    def discard_exceeding(self, max_tokens=None, cur_tokens=None):
        raise NotImplementedError

    def calc_tokens(self):
        raise NotImplementedError


class SessionManager(object):
    """这里主要是更新真实的上下文缓存"""

    def __init__(self, sessioncls, **session_args):
        self.sessions = redis_storage._redis
        self.sessioncls = sessioncls
        self.session_args = session_args
        self.expires_in_seconds = int(get_env("SESSION_EXPIRES_IN_SECONDS", 3600))

    def build_session(self, session_id, system_prompt=None):
        if session_id is None:
            return self.sessioncls(session_id, system_prompt, **self.session_args)
        
        session_json = self.sessions.get(session_id)
        if session_json is None:
            session = self.sessioncls(session_id, system_prompt, **self.session_args)
            self.sessions.set(session_id, session.to_json())
        else:
            session = Session.from_json(session_json)
            if system_prompt is not None:
                session.set_system_prompt(system_prompt)
                self.sessions.setex(session_id, self.expires_in_seconds, session.to_json())
        
        return session

    def session_query(self, query, session_id):
        session = self.build_session(session_id, None)
        session.add_query(query)
        self.sessions.setex(session_id, self.expires_in_seconds, session.to_json())
        try:
            max_tokens = get_env("BOT_CONVERSATION_MAX_TOKENS", 1000)
            total_tokens = session.discard_exceeding(max_tokens, None)
            logger.debug("prompt tokens used={}".format(total_tokens))
        except Exception as e:
            logger.debug("Exception when counting tokens precisely for prompt: {}".format(str(e)))
        return session

    def session_reply(self, reply, session_id, total_tokens=None):
        session = self.build_session(session_id, None)
        session.add_reply(reply)
        self.sessions.setex(session_id, self.expires_in_seconds, session.to_json())    
        try:
            max_tokens = get_env("BOT_CONVERSATION_MAX_TOKENS", 1000)
            tokens_cnt = session.discard_exceeding(max_tokens, total_tokens)
            logger.debug("raw total_tokens={}, savesession tokens={}".format(total_tokens, tokens_cnt))
        except Exception as e:
            logger.debug("Exception when counting tokens precisely for session: {}".format(str(e)))
        return session

    def clear_session(self, session_id):
        self.sessions.delete(session_id)

    def clear_all_session(self):
        # 警告：这个操作会清除Redis数据库中所有的数据
        self.sessions.flushdb()