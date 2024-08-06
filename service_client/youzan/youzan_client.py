import httpx
import asyncio
import json

class YouzanClient:
    def __init__(self):
        self.base_url = "https://open.youzanyun.com/api/"  # 假设的API基础URL
        self.client = httpx.AsyncClient()
    
    async def generate_token(self):
        base_url = "http://api.lubanactivity.skg.cc"
        url = f"{base_url}/tradeIn/api/auth/youzan_token"
        payload = {
            "appKey": "f1440077342a8ec1c9",
            "appSecret": "32e626a68a56d19b48811c0d23f1b435"
        }
        test_payload = {
            "appKey": "activity",
            "appSecret": "123456"
        } 
        try:
            print(f"url {url} json {payload}")
            response = await self.client.post(url, json=test_payload)
            print(f"response {response.__dict__}")
            if response.status_code == 200:
                res= response.json()
                print(f"res {res}")
                if res.get("success"):
                    token = res.get('result', {}).get('youzan_token')
                    return token
        except httpx.RequestError as e:
            print(f"Request failed generate_token: {str(e)}")
        except Exception as e:
            print(f"An error occurred generate_token: {str(e)}")
        return

    async def call_api(self, api, version, params):
        """
        异步调用有赞云API。

        :param api: API端点。
        :param version: API版本。
        :param params: 请求的参数。
        :return: API响应数据。
        """
        url = f"{self.base_url}/{api}/{version}"
        try:
            token = await self.generate_token()
            playload = {'access_token': token, **params}
            response = await self.client.get(url, params=playload)
            if response.status_code == 200:
                return response.json()
        except httpx.RequestError as e:
            print(f"Request failed: {str(e)}")
        except Exception as e:
            print(f"An error occurred: {str(e)}")
        return None
    
    async def search_customer(self, page, card_alias):
        """
        搜索客户信息。

        :param page: 页码。
        :param card_alias: 卡别名。
        :return: 客户信息或错误消息。
        """
        api = 'youzan.scrm.customer.search'
        version = '3.0.0'
        params = {
            "page": page,
            "card_alias": card_alias
        }
        return await self.call_api(api, version, params)

    async def close(self):
        """
        关闭HTTP客户端。
        """
        await self.client.aclose()

# 使用示例
async def main():
    client = YouzanClient()
    response = await client.search_customer(page=1, card_alias='Y2op16up7ka5hs')
    print(json.dumps(response, indent=4))
    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
