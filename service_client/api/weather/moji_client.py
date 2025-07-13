import json
import urllib.parse
from typing import Dict, Any, Optional

import httpx

from common_sdk.logging.logger import logger
from common_sdk.system.function_timer import function_timer


class MojiWeatherClient:
    """墨迹天气API客户端。

    提供异步方式访问墨迹天气API的功能，包括城市天气查询和IP定位天气查询。

    Attributes:
        host: API服务器域名
        path: API路径
        app_code: 授权AppCode
        token: API访问令牌
    """
    def __init__(self, app_code):
        """初始化墨迹天气API客户端。

        Args:
            app_code: 阿里云市场授权的AppCode
            token: API访问令牌，默认使用通用令牌
        """
        self.host = "http://aliv8.data.moji.com"
        self.path = "/whapi/json/aliweather/condition"
        self.app_code = app_code

    async def _request(self, params: Dict[str, Any], path: Optional[str] = None) -> Dict[str, Any]:
        """发送异步请求到墨迹天气API。

        Args:
            params: 请求参数字典
            path: API路径，默认使用实例的path属性

        Returns:
            Dict[str, Any]: API响应结果

        Raises:
            RuntimeError: 请求失败时抛出异常
        """
        url = f"{self.host}{path or self.path}"

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Authorization': f'APPCODE {self.app_code}'
        }

        # 添加通用参数

        try:
            logger.info(f"墨迹天气API请求: URL={url}, headers={headers},参数={params}")

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url,
                    data=params,
                    headers=headers,
                    timeout=30
                )

            if response.status_code != 200:
                logger.error(f"墨迹天气API请求失败: 状态码={response.status_code}, 响应={response.text}")
                raise RuntimeError(f"墨迹天气API请求失败: HTTP {response.status_code}")

            result: Dict[str, Any] = response.json()
            logger.info(f"墨迹天气API响应: {result}")
            return result

        except Exception as e:
            logger.error(f"墨迹天气API请求异常: {str(e)}")
            raise RuntimeError(f"墨迹天气API请求异常: {str(e)}")

    @function_timer("【墨迹天气】-城市天气查询")
    async def get_weather_info(self, city_id: str) -> Dict[str, Any]:
        """根据城市ID获取天气信息。

        Args:
            city_id: 城市ID

        Returns:
            Dict[str, Any]: 包含天气信息的响应

        Raises:
            RuntimeError: 请求失败时抛出异常
        """
        params = {'cityId': city_id}
        return await self._request(params)

    @function_timer("【墨迹天气】-IP定位天气查询")
    async def get_weather_info_by_ip(self, ip_address: str) -> Dict[str, Any]:
        """根据IP地址获取天气信息。

        Args:
            ip_address: IP地址

        Returns:
            Dict[str, Any]: 包含天气信息的响应

        Raises:
            RuntimeError: 请求失败时抛出异常
        """
        params = {'ip': ip_address}
        return await self._request(params)

    @function_timer("【墨迹天气】-经纬度天气查询")
    async def get_weather_info_by_location(self, latitude: float, longitude: float) -> Dict[str, Any]:
        """根据经纬度获取天气信息。

        Args:
            latitude: 纬度
            longitude: 经度

        Returns:
            Dict[str, Any]: 包含天气信息的响应

        Raises:
            RuntimeError: 请求失败时抛出异常
        """
        params = {
            'lat': str(latitude),
            'lon': str(longitude)
        }
        return await self._request(params, path=self.path)
