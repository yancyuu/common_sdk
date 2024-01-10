#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""封装Openapi基础操作"""
import asyncio
import time
import httpx
import orjson
from typing import Optional
from pydantic import BaseModel, root_validator
from common_sdk.service_client.lingxing.sign import SignBase
from typing import Any, Optional

def reset_msg_and_trace_id(cls, values: dict):
    """重置异常信息"""
    try:
        values['message'] = values.get('message') or values.get('msg', '')
        values['request_id'] = values.get('request_id') or values.get('traceId', '')
    except Exception as e:
        pass
    return values


class ResponseResult(BaseModel):
    code: Optional[int]                     # 响应码
    message: Optional[str]                  # 响应信息
    data: Any                               # 接口响应数据
    error_details: Optional[Any] = None     # 异常信息
    request_id: Optional[str] = None        # 标记本次请求唯一ID
    response_time: Optional[str] = None     # 响应时间
    total: Optional[int] = None

    _reset_msg_and_trace_id = root_validator(allow_reuse=True, pre=True)(
        reset_msg_and_trace_id
    )


class AccessTokenDto(BaseModel):
    access_token: str           # 接口访问认证信息
    refresh_token: str          # RefreshToken用于续费AccessToken，只能使用一次
    expires_in: int             # AccessToken的有效期, TTL


class LingxingClient(object):

    def __init__(self, app_id: str, app_secret: str, default_timeout=30):
        self.host = "https://openapi.lingxing.com"
        self.app_id = app_id
        self.app_secret = app_secret
        self.default_timeout = default_timeout

    async def request(self, route_name: str, method: str,
                      req_params: Optional[dict] = None,
                      req_body: Optional[dict] = None,
                      **kwargs) -> ResponseResult:
        req_url = self.host + route_name
        headers = kwargs.pop('headers', {})
        # 获取 access_token
        try:
            access_token = await self.generate_access_token()
        except Exception as e:
            raise ValueError(f"Error getting access token: {e}")
        # 准备签名参数
        timestamp = f'{int(time.time())}'
        gen_sign_params = {
            "app_key": self.app_id,
            "access_token": access_token.access_token,
            "timestamp": timestamp,
            **(req_params or {}),
            **(req_body or {})
        }
        sign = SignBase.generate_sign(self.app_id, gen_sign_params)
        
        # 更新请求参数
        req_params = {
            **gen_sign_params,
            "sign": sign
        }

        # 设置默认的Content-Type
        if req_body and 'Content-Type' not in headers:
            headers['Content-Type'] = 'application/json'

        # 发送请求
        return await self.make_request(method, req_url, params=req_params,
                                        headers=headers, json=req_body, **kwargs)

            
    async def make_request(self, method: str, req_url: str,
                           params: Optional[dict] = None,
                           json: Optional[dict] = None,
                           headers: Optional[dict] = None,
                           **kwargs) -> ResponseResult:
        timeout = kwargs.pop('timeout', self.default_timeout)
        data = orjson.dumps(json, option=orjson.OPT_SORT_KEYS).decode() if json else None
        async with httpx.AsyncClient() as client:
            print(f"request---------------------->{req_url} {params} {data} {headers}")

            resp = await client.request(method=method, url=req_url, params=params, 
                                        content=data, timeout=timeout, headers=headers)
            print(f"resp---------------------->{resp.json()}")
            if resp.status_code != 200:
                raise ValueError(f"Response error, status code: {resp.status_code}, body: {resp.text}")
            return ResponseResult(**resp.json())
    
    async def _make_token_request(self, path: str, additional_params: dict) -> AccessTokenDto:
        """构造和发送获取/刷新token的请求"""
        req_url = self.host + path
        req_params = {"appId": self.app_id, "appSecret": self.app_secret, **additional_params}
        resp_result = await self.make_request("POST", req_url, params=req_params)
        if resp_result.code != 200:
            error_msg = f"generate_access_token failed, reason: {resp_result.message}"
            raise ValueError(error_msg)
        assert isinstance(resp_result.data, dict)
        return AccessTokenDto(**resp_result.data)

    async def generate_access_token(self) -> AccessTokenDto:
        return await self._make_token_request('/api/auth-server/oauth/access-token', {})

    async def refresh_token(self, refresh_token: str) -> AccessTokenDto:
        return await self._make_token_request('/api/auth-server/oauth/refresh', {"refreshToken": refresh_token})



if __name__ == "__main__":    
    async def main():
        api_client = LingxingClient(app_id="ak_oZqggBoEB3k7q", app_secret="HpzInsnyQr6vDJOeuIBb7w==")
        result = await api_client.request("/erp/sc/data/seller/lists", "GET")
        print(result)
    # 运行异步主函数
    asyncio.run(main())