import backoff
import httpx
import json
from ...logging.logger import logger
from typing import Any, Dict


async def process_response(response: httpx.Response) -> Dict[str, Any]:
    if response.status_code == 200:
        data = response.json()
        logger.info(f"[LLM] response {data}")
        return data
    else:
        logger.info(f"[LLM] response code {response.status_code} request {response.__dict__}")
        raise httpx.HTTPStatusError(f"Error response {response.status_code}", request=response.request, response=response)


class BaseClient:

    def __init__(self, api_key: str, base_url: str):
        self.base_url = base_url
        self.api_key = api_key

    @staticmethod
    def generate_curl_command(url, method, headers=None, json_data=None, params=None, files=None):
        # 初始命令
        curl_command = f"curl -X {method.upper()} {url}"

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
        (httpx.HTTPStatusError, httpx.TimeoutException),
        max_tries=5,
        giveup=lambda e: e.response is not None and e.response.status_code < 500
    )
    async def make_request_async(self, method: str, url: str, body: Any = None, params: Dict[str, Any] = None, files: Any = None, timeout=60) -> Dict[str, Any]:
        async with httpx.AsyncClient() as client:
            headers = self.get_headers()
            self.generate_curl_command(url, method, headers=headers, json_data=body, params=params, files=files)
            response = await client.request(
                method=method,
                url=self.base_url + url,
                headers=headers,
                json=body,
                files=files,
                params=params,
                timeout = timeout
            )
            return await process_response(response)
        
    @backoff.on_exception(
            backoff.expo,
            (httpx.HTTPStatusError, httpx.TimeoutException),
            max_tries=5,
            giveup=lambda e: e.response is not None and e.response.status_code < 500
        )
    async def make_request_stream_async(self, method: str, url: str, body: Any = None, params: Dict[str, Any] = None, files: Any = None, timeout=60) -> Any:
        headers = self.get_headers()
        async with httpx.AsyncClient() as client:
            try:
                self.generate_curl_command(url, method, headers=headers, json_data=body, params=params, files=files)
                async with client.stream(method, self.base_url + url, headers=headers, json=body, timeout=timeout, files=files) as response:
                    response.raise_for_status()
                    async for chunk in response.aiter_text():
                        yield chunk
            except httpx.TimeoutException as e:
                logger.error(f'请求超时，错误信息：{e}')
                raise
            except httpx.HTTPStatusError as e:
                logger.error(f'请求失败，状态码：{e.response.status_code}, 错误信息：{e.response.text}')
                raise
        

    
    
