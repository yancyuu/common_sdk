import backoff
import httpx
from common_sdk.system.sys_env import get_env
from common_sdk.logging.logger import logger
from typing import Any, Dict, List, TypeVar, Generic, Protocol, runtime_checkable
        
# 定义泛型
T = TypeVar('T', bound='BaseAPI')

class BaseAPI:

    def __init__(self, api_key: str, base_url: str):
        self.base_url = base_url
        self.api_key = api_key

    def get_headers(self):
        return {
            'Content-Type': 'application/json; charset=UTF-8',
            'Authorization': f'Bearer {self.api_key}'
        }
    
    async def process_response(self, response: httpx.Response) -> Dict[str, Any]:
        data = response.json()
        if response.status_code == 200:
            return data
        else:
            raise httpx.HTTPStatusError(f"Error response {response.status_code}", request=response.request, response=response)

    @backoff.on_exception(
        backoff.expo,
        (httpx.HTTPStatusError, httpx.TimeoutException),
        max_tries=5,
        giveup=lambda e: e.response is not None and e.response.status_code < 500
    )
    async def make_request_async(self, method: str, url: str, body: Any = None, params: Dict[str, Any] = None, files: Any = None, timeout=60) -> Dict[str, Any]:
        async with httpx.AsyncClient() as client:
            response = await client.request(
                method=method,
                url=self.base_url + url,
                headers=self.get_headers(),
                json=body,
                files=files,
                params=params,
                timeout = timeout
            )
            return await self.process_response(response)
        
    @backoff.on_exception(
            backoff.expo,
            (httpx.HTTPStatusError, httpx.TimeoutException),
            max_tries=5,
            giveup=lambda e: e.response is not None and e.response.status_code < 500
        )
    async def make_request_stream_async(self, method, url, body=None, timeout=60):
        headers = self.get_headers()
        async with httpx.AsyncClient() as client:
            try:
                async with client.stream(method, self.base_url + url, headers=headers, json=body, timeout=timeout) as response:
                    response.raise_for_status()
                    async for chunk in response.aiter_text():
                        yield chunk
            except httpx.TimeoutException as e:
                logger.error(f'请求超时，错误信息：{e}')
                raise
            except httpx.HTTPStatusError as e:
                logger.error(f'请求失败，状态码：{e.response.status_code}, 错误信息：{e.response.text}')
                raise
        

    
    
