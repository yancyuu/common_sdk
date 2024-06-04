from ..util.redis_utils import redis_storage
from ..system.sys_env import get_env
from ..logging.logger import logger

"""人工锁"""
class ManualLock:

    LOCK_KEY_SUFFIX = "MANUAL_LOCK"

    def __init__(self, expires_in_seconds):
        self.client = redis_storage._redis
        self.expires_in_seconds = expires_in_seconds

    def acquire_lock(self, lock_name):
        # 尝试设置锁，使用nx确保只有在锁不存在的时候才设置成功
        result = self.client.set(lock_name, 1, nx=True, ex=self.expires_in_seconds)
        return result is not None  # 如果设置成功，返回True

    def is_lock_active(self, lock_name):
        # 查询锁状态，如果锁存在，返回True
        return self.client.get(lock_name) is not None

    def refresh_lock(self, lock_name):
        if self.client.exists(lock_name):
            self.client.expire(lock_name, self.expires_in_seconds)

    def release_lock(self, lock_name):
        self.client.delete(lock_name)
    
    def lock(self, lock_name):
        is_lock = False
        # 已经被锁
        has_lock = False
        # 如果已经被锁，说明此用户在人工
        if not self.acquire_lock(lock_name):
            has_lock=True
        else:
            self.acquire_lock(lock_name)
            is_lock=True
        logger.info(f"锁状态   {lock_name} 此次是否上锁{is_lock}  是否已经上锁{has_lock}")
        return is_lock, has_lock

manual_lock = ManualLock(get_env("MANUAL_LOCK_EXPIRES_IN_SECONDS", 30))