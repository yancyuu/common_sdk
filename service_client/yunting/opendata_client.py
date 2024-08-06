import asyncio
import backoff
from common_sdk.system.sys_env import get_env
from common_sdk.logging.logger import logger
from datetime import datetime, timedelta
import httpx
import traceback


class YuntingAPIException(Exception):
    """自定义异常类用于处理云听API返回的错误"""
    pass


class OpendataClient:
    """
    每天请求前一天的云听数据补全到飞书中，做兜底策略
    """
    def __init__(self, source, third_party_id):
        self.base_url = get_env("YUNTING_OPEANDATA_BASE_URL")
        self.source = source
        self.third_party_id = third_party_id
        self.resource_token = get_env("YUNTING_OPEANDATA_RESOURCE_TOKEN")

    @backoff.on_exception(backoff.expo, (httpx.HTTPStatusError, YuntingAPIException), base=2, factor=2, max_time=60, jitter=backoff.full_jitter, max_tries=5)
    async def fetch_yunting_data(self):
        await self.get_access_token()
        # 查询数据开始时间，精确到毫秒 历史模式：从0开始赋值开始拉 正常模式：从昨天凌晨0点赋值开始拉
        # 云听给我们设置的模式是历史模式，所以这里的data_insert_timestamp先传0
        #yesterday = datetime.now() - timedelta(days=1)
        #data_insert_timestamp = int(yesterday.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
        data_insert_timestamp = 0
        all_data = []

        while True:
            payload = {
                "token": self.resource_token,
                "dataType": "comment",
                "dataInsertTimestamp": data_insert_timestamp,
                "isCompress": False
            }

            headers = {
                "content-type": "application/json;charset=UTF-8",
                "x-auth": self.token
            }

            async with httpx.AsyncClient() as client:
                # 打印curl格式的请求命令
                curl_command = f"curl -X POST {self.base_url + '/v2/third/pull'} -H 'Content-Type: application/json' -H 'x-auth: {self.token}' -d '{payload}'"
                logger.info(f"[YUNTING] request {curl_command}")

                response = await client.post(self.base_url + "/v2/third/pull", json=payload, headers=headers)
                logger.info(f"[YUNTING] response {response.__dict__}")

                response.raise_for_status()
                response = response.json()
                if response.get("code") != 20000:
                    raise YuntingAPIException("云听接口返回失败")
                # 解析响应数据
                fetched_data = response.get("result", {})
                data = fetched_data.get("data")
                all_data.extend(data)

                if len(data) < 1000:
                    break

                # 更新 dataInsertTimestamp 为下一个请求的开始时间
                data_insert_timestamp = fetched_data.get("dataInsertTimestamp")

                # 添加延迟，确保每分钟不超过30次请求
                await asyncio.sleep(2)  # 每次请求后延迟2秒

        return all_data

    @backoff.on_exception(backoff.expo, httpx.HTTPStatusError, max_tries=3)
    async def get_access_token(self):
        async with httpx.AsyncClient() as client:
            response = await client.get(url=self.base_url + "/oauth2/token", 
                                        params={"source": self.source,
                                                "third_party_id": self.third_party_id})
            response.raise_for_status()
            token_data = response.json()
            logger.info(f"[YUNTING] token_data {token_data}")
            if token_data.get("code") != 20000:
                raise YuntingAPIException("token接口返回失败")
            self.token = token_data.get("result", {}).get("access_token")
            if not self.token:
                raise YuntingAPIException("接口未返回正常token")


