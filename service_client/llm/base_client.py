import aiohttp
import asyncio
import json
from ...logging.logger import logger
from typing import Any, Dict
import backoff


# 处理响应
async def process_response(response: aiohttp.ClientResponse) -> Dict[str, Any]:
    if response.status == 200:
        data = await response.json()
        logger.info(f"[LLM] response {data}")
        return data
    else:
        logger.info(f"[LLM] response code {response.status} request {response.__dict__}")
        raise aiohttp.ClientResponseError(
            request_info=response.request_info,
            history=response.history,
            status=response.status,
            message=f"Error response {response.status}",
        )


# BaseClient 类
class BaseClient:

    def __init__(self, api_key: str, base_url: str, max_connections: int = 200):
        self.base_url = base_url
        self.api_key = api_key
        self.connector = aiohttp.TCPConnector(limit=max_connections, keepalive_timeout=30, ttl_dns_cache=300)
        self.session = aiohttp.ClientSession(connector=self.connector)

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()  # 在程序结束或实例销毁时关闭

    # 生成 curl 命令
    def generate_curl_command(self, url, method, headers=None, json_data=None, params=None, files=None):
        curl_command = f"curl -X {method.upper()} {self.base_url}{url}"

        if headers:
            for key, value in headers.items():
                curl_command += f" -H '{key}: {value}'"

        if json_data:
            json_str = json.dumps(json_data)
            curl_command += f" -d '{json_str}'"

        if params:
            param_str = "&".join([f"{key}={value}" for key, value in params.items()])
            curl_command += f"?{param_str}"

        if files:
            for field_name, file_path in files.items():
                curl_command += f" -F '{field_name}=@{file_path}'"

        logger.info(f"[LLM] request {curl_command}")
        return curl_command

    # 获取请求头
    def get_headers(self):
        return {
            'Content-Type': 'application/json; charset=UTF-8',
            'Authorization': f'Bearer {self.api_key}'
        }

    # 普通异步请求，确保每次请求后关闭 client session
    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientResponseError, asyncio.TimeoutError),
        max_tries=5,
        giveup=lambda e: e.status < 500
    )
    async def make_request_async(self, method: str, url: str, body: Any = None, params: Dict[str, Any] = None,
                                 files: Any = None, timeout=300) -> Dict[str, Any]:
        headers = self.get_headers()
        self.generate_curl_command(url, method, headers=headers, json_data=body, params=params, files=files)
        # 创建并关闭 session 在请求内完成
        async with aiohttp.ClientSession(connector=self.connector) as session:  # 每次创建一个新的 ClientSession
            try:
                async with session.request(
                        method=method,
                        url=self.base_url + url,
                        headers=headers,
                        json=body,
                        data=files,
                        params=params,
                        timeout=timeout
                ) as response:
                    return await process_response(response)
            except asyncio.TimeoutError as e:
                logger.error(f'请求超时，错误信息：{e}')
                raise
            except aiohttp.ClientResponseError as e:
                logger.error(f'请求失败，状态码：{e.status}, 错误信息：{await response.text()}')
                raise

    # 流式异步请求，复用了client session，需要每次请求后手动关闭 client session
    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientResponseError, asyncio.TimeoutError),
        max_tries=5,
        giveup=lambda e: e.status < 500
    )
    async def make_request_stream_async(self, method: str, url: str, body: Any = None, params: Dict[str, Any] = None,
                                        files: Any = None, timeout=300) -> Any:
        headers = self.get_headers()
        self.generate_curl_command(url, method, headers=headers, json_data=body, params=params, files=files)
        try:
            async with self.session.request(
                    method=method,
                    url=self.base_url + url,
                    headers=headers,
                    json=body,
                    data=files,
                    params=params,
                    timeout=timeout
            ) as response:
                response.raise_for_status()
                async for chunk in response.content.iter_chunked(16384):  # 处理流式输出
                    logger.debug(f"[LLM] Streaming chunk: {chunk}")
                    yield chunk
        except asyncio.TimeoutError as e:
            logger.error(f'请求超时，错误信息：{e}')
            raise
        except aiohttp.ClientResponseError as e:
            logger.error(f'请求失败，状态码：{e.status}, 错误信息：{await response.text()}')
            raise
