from ..util.redis_utils import redis_storage
from ..system.sys_env import get_env
from ..logging.logger import logger
import json

"""澄清锁"""
class ClarificationLock:

    CLARIFICATION_KEY_SUFFIX = "CLARIFICATION_LOCK_"
    CLARIFICATION_KEY_PRODUCT_SUFFIX = "CLARIFICATION_LOCK_PRODUCT_"  # 产品澄清的前缀


    def __init__(self):
        self.client = redis_storage._redis
        # 澄清锁和会话过期时间保持一致
        self.expires_in_seconds = get_env("SESSION_EXPIRES_IN_SECONDS")

    import json

    def acquire_lock(self, lock_name, clarification_data=None):
        # 只有在提供了澄清数据时，才执行存储操作
        if clarification_data is not None:
            value = json.dumps(clarification_data)  # 将澄清数据序列化为JSON字符串
            result = self.client.set(lock_name, value, nx=True, ex=self.expires_in_seconds)
            return result is not None
        # 如果没有提供澄清数据，直接返回False，表示没有获取到锁
        return False

    def is_lock_active(self, lock_name):
        # 查询锁状态，如果锁存在，返回True
        return self.client.get(lock_name) is not None
    
    import json

    def get_clarification_data(self, lock_name):
        # 从Redis获取数据
        data = self.client.get(lock_name)
        if data:
            try:
                # 尝试反序列化JSON数据
                return json.loads(data)
            except json.JSONDecodeError:
                # 如果数据不是有效的JSON格式，可能是其他类型的锁，直接返回原始数据
                return None
        # 如果数据不存在，或锁不存在，则返回None
        return None

    def refresh_lock(self, lock_name):
        if self.client.exists(lock_name):
            self.client.expire(lock_name, self.expires_in_seconds)

    def release_lock(self, lock_name):
        self.client.delete(lock_name)

    def lock(self, lock_name, clarification_data=None):
        is_lock = False
        has_lock = False
        if not self.acquire_lock(lock_name, clarification_data):
            has_lock = True
        else:
            is_lock = True

        logger.info(f"锁状态 {lock_name} 此次是否上锁{is_lock} 是否已经上锁{has_lock}")
        return is_lock, has_lock
    

clarification_lock = ClarificationLock()