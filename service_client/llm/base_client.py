import aiohttp
import asyncio
import json
from ...logging.logger import logger
from typing import Any, Dict
import backoff


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


class BaseClient:

    def __init__(self, api_key: str, base_url: str):
        self.base_url = base_url
        self.api_key = api_key
        self.session = None

    async def __aenter__(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def generate_curl_command(self, url, method, headers=None, json_data=None, params=None, files=None):
        # 初始命令
        curl_command = f"curl -X {method.upper()} {self.base_url}/{url}"

        # 添加请求头
        if headers:
            for key, value in headers.items():
                curl_command += f" -H '{key}: {value}'"

        # 添加 JSON 数据
        if json_data:
            json_str = json.dumps(json_data)
            curl_command += f" -d '{json_str}'"

        # 添加查询参数
        if params:
            param_str = "&".join([f"{key}={value}" for key, value in params.items()])
            curl_command += f"?{param_str}"

        # 添加文件
        if files:
            for field_name, file_path in files.items():
                curl_command += f" -F '{field_name}=@{file_path}'"

        # 日志记录 curl 命令
        logger.info(f"[LLM] request {curl_command}")

        return curl_command

    def get_headers(self):
        return {
            'Content-Type': 'application/json; charset=UTF-8',
            'Authorization': f'Bearer {self.api_key}'
        }

    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientResponseError, asyncio.TimeoutError),
        max_tries=5,
        giveup=lambda e: e.status < 500
    )
    async def make_request_async(self, method: str, url: str, body: Any = None, params: Dict[str, Any] = None,
                                 files: Any = None, timeout=60) -> Dict[str, Any]:
        headers = self.get_headers()
        self.generate_curl_command(url, method, headers=headers, json_data=body, params=params, files=files)
        try:
            if not self.session or self.session.closed:
                await self.__aenter__()  # 确保会话是开启状态
            async with self.session.request(
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

    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientResponseError, asyncio.TimeoutError),
        max_tries=5,
        giveup=lambda e: e.status < 500
    )
    async def make_request_stream_async(self, method: str, url: str, body: Any = None, params: Dict[str, Any] = None,
                                        files: Any = None, timeout=60) -> Any:
        headers = self.get_headers()
        self.generate_curl_command(url, method, headers=headers, json_data=body, params=params, files=files)
        try:
            if not self.session or self.session.closed:
                await self.__aenter__()  # 确保会话是开启状态
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
                async for chunk in response.content.iter_chunked(512):
                    yield chunk
        except asyncio.TimeoutError as e:
            logger.error(f'请求超时，错误信息：{e}')
            raise
        except aiohttp.ClientResponseError as e:
            logger.error(f'请求失败，状态码：{e.status}, 错误信息：{await response.text()}')
            raise
