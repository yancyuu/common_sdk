import asyncio
import httpx
import time
import backoff
from common_sdk.system.sys_env import get_env
from common_sdk.logging.logger import logger

class FastGPTClient:
    """
     FastGPT的操作类
    """
    def __init__(self, api_key):
        self.base_url = get_env("FASTGPT_BASE_URL", "https://www.lazygpt.cn")
        # 初始化的时候传入API密钥，因为API密钥和应用相关
        self.api_key = api_key  # 假设API密钥通过逗号分隔的字符串提供

    def get_headers(self):
        return {
            'Content-Type': 'application/json; charset=UTF-8',
            'Authorization': f'Bearer {self.api_key}'
        }
    
    @backoff.on_exception(backoff.expo,
                      (httpx.HTTPStatusError, httpx.TimeoutException),
                      base=3,
                      factor=2,
                      max_tries=5,
                      max_time=30)
    async def make_request_stream_async(self, method, url, body=None, timeout=30):
        headers = self.get_headers()
        full_url = f'{self.base_url}{url}'
        async with httpx.AsyncClient() as client:
            try:
                # Properly using the async with statement with client.stream
                async with client.stream(method, full_url, headers=headers, json=body, timeout=timeout) as response:
                    response.raise_for_status()  # Ensure the response status is OK
                    async for chunk in response.aiter_text():
                        yield chunk
            except httpx.TimeoutException as e:
                logger.error(f'请求超时，错误信息：{e}')
                raise
            except httpx.HTTPStatusError as e:
                logger.error(f'请求失败，状态码：{e.response.status_code}, 错误信息：{e.response.text}')
                # if e.response.status_code == 500:
                #     retry_after = int(e.response.headers.get("Retry-After", 30))
                #     logger.info(f"已达到速率限制，将在{retry_after}秒后重试...")
                #     await asyncio.sleep(retry_after)
                #     async for chunk in self.make_request_stream_async(method, url, body, timeout):
                #         yield chunk
                # else:
                raise

    @backoff.on_exception(backoff.expo,
                          (httpx.HTTPStatusError, httpx.TimeoutException),
                          base=3,
                          factor=2,
                          max_tries=3,
                          max_time=30)
    async def make_request_async(self, method, url, body=None, timeout=30):
        headers = self.get_headers()
        full_url = f'{self.base_url}{url}'
        async with httpx.AsyncClient() as client:
            try:
                logger.info(f"请求地址：{full_url}, headers {headers} json {body} timeout {timeout}")
                response = await client.request(method, full_url, headers=headers, json=body, timeout=timeout)
                response.raise_for_status()
                return self.process_response(response)
            except httpx.TimeoutException as e:  # 捕获超时异常
                logger.error(f'请求超时，错误信息：{e}')
                raise
            except httpx.HTTPStatusError as e:  # 捕获异常
                logger.error(f'请求失败，状态码：{e.response.status_code}, 错误信息：{e.response.text}') 
                # if e.response.status_code == 500:
                #     # 从响应中获取等待时间，如果没有提供，则默认等待60秒
                #     retry_after = int(e.response.headers.get("Retry-After", 30))
                #     logger.info(f"已达到速率限制，将在{retry_after}秒后重试...")
                #     await asyncio.sleep(retry_after)  # 异步等待指定的时间
                #     return await self.make_request_async(method, url, body, timeout)  # 重新发起请求
                # # 在这里，你可以获取到异常的详细信息
                # else:
                raise 
        
    @backoff.on_exception(backoff.expo,
                          (httpx.HTTPStatusError, httpx.TimeoutException),
                          base=3,
                          factor=2,
                          max_tries=5,
                          max_time=30)
    def make_request_sync(self, method, url, body=None, timeout=30):
        headers = self.get_headers()
        full_url = f'{self.base_url}{url}'
        with httpx.Client() as client:
            try:
                response = client.request(method, full_url, headers=headers, json=body, timeout=timeout)
                response.raise_for_status()  # 如果状态码指示错误，这里将抛出异常
                return self.process_response(response)
            except httpx.TimeoutException as e:  # 捕获超时异常
                logger.error(f'请求超时，错误信息：{e}')
                raise
            except httpx.HTTPStatusError as e:  # 捕获异常
                # 在这里，你可以获取到异常的详细信息 
                logger.error(f'请求失败，状态码：{e.response.status_code}, 错误信息：{e.response.text}') 
                # if e.response.status_code == 500:
                #     # 从响应中获取等待时间，如果没有提供，则默认等待60秒
                #     retry_after = int(e.response.headers.get("Retry-After", 30))
                #     logger.info(f"已达到速率限制，将在{retry_after}秒后重试...")
                #     time.sleep(retry_after)  # 异步等待指定的时间
                #     return self.make_request_sync(method, url, body, timeout)  # 重新发起请求
                # # 在这里，你可以获取到异常的详细信息
                # else:
                raise 

    def process_response(self, response):
        try:
            # 尝试从响应中获取 JSON 数据
            json_data = response.json()
            # 尝试从 JSON 数据中获取 'data' 字段
            if 'data' in json_data:
                return json_data['data']
            # 尝试从 JSON 数据中获取 'choices' 字段
            elif 'choices' in json_data:
                return json_data['choices'][0]['message']
            else:
                return None
        except Exception as e:
            # 如果在处理过程中发生异常（例如，响应无法解析为 JSON），则记录或处理异常
            logger.info(f"处理fastgpt响应时发生异常: {e}")
            # 并返回空
            return None

    async def search_async(self, dataset_id, text, limit=10, similarity=0, search_mode="embedding", using_re_rank=False):
        payload = {
            "datasetId": dataset_id,
            "text": text,
            "limit": limit,
            "similarity": similarity,
            "searchMode": search_mode,
            "usingReRank": using_re_rank
        }
        return await self.make_request_async('POST', '/api/core/dataset/searchTest', payload, 10)

    def search(self, dataset_id, text, limit=5000, similarity=None, search_mode="embedding", using_re_rank=True):
        """
        search _summary_

        Args:
            dataset_id: 知识库ID
            text: 需要测试的文本
            limit: 最大 tokens 数量 默认 5000.
            similarity:最低相关度（0~1，可选）
            search_mode: 搜索模式：embedding | fullTextRecall | mixedRecall
            using_re_rank: 使用重排

        Returns:
            返回查找到的数据
        """
        payload = {
            "datasetId": dataset_id,
            "text": text,
            "limit": limit,
            "similarity": similarity,
            "searchMode": search_mode,
            "usingReRank": using_re_rank
        }
        return self.make_request_sync('POST', '/api/core/dataset/searchTest', payload, 10)

    async def chat_completions(self, payload, chat_id=None, detail=False):
        if detail:
            payload.update({"detail":detail})
        if chat_id:
            payload.update({"chatId": chat_id})
        return await self.make_request_async('POST', '/api/v1/chat/completions', payload, timeout=60)
    
    async def chat_completions_stream(self, payload,chat_id=None, detail=False):
        payload.update({"stream":True})
        if detail:
            payload.update({"detail":detail})
        if chat_id:
            payload.update({"chatId": chat_id})
    
        # 流式请求，需要逐个产出数据
        async for chunk in self.make_request_stream_async('POST', '/api/v1/chat/completions', payload, timeout=60):
            yield chunk
    

    async def async_push_for_collection(self, collection_id, data_list, mode='chunk'):
        url = '/api/core/dataset/data/pushData'
        payload = {
            'collectionId': collection_id,
            'mode': mode,
            'data': data_list
        }
        return await self.make_request_async('POST', url, payload)
    
    async def async_listall_for_collection(self, collection_id, search_text=""):
        """
        异步获取集合中的所有数据，支持搜索文本。

        Args:
            collection_id (str): 集合的 ID。
            search_text (str, optional): 搜索文本，默认为空字符串。

        Returns:
            list: 集合中所有满足条件的数据项。
        """
        page_num = 1
        page_size = 10  # 可以根据实际情况调整每页的大小
        all_data = []
        total_collected = 0
        total_items = None

        while total_items is None or total_collected < total_items:
            # 构造请求的 payload
            payload = {
                "pageNum": page_num,
                "pageSize": page_size,
                "collectionId": collection_id,
                "searchText": search_text
            }

            # 发送异步 POST 请求并获取响应
            response = await self.make_request_async('POST', '/api/core/dataset/data/list', payload, timeout=10)

            # 解析响应
            page_data = response.get('data', [])
            total_items = response.get('total', 0)
            
            all_data.extend(page_data)
            total_collected += len(page_data)

            # 准备请求下一页
            page_num += 1

        return all_data

    async def listall_collections(self, dataset_id):
        if not dataset_id:
            raise ValueError("Dataset ID is required.")

        url = '/api/core/dataset/collection/list'
        all_collections = []
        page_num = 1
        page_size = 10  # 或者根据您的实际情况调整页大小

        while True:
            payload = {
                "pageNum": page_num,
                "pageSize": page_size,
                "datasetId": dataset_id,
                "searchText": ""
            }
            response = await self.make_request_async('POST', url, body=payload)
            collections = response.get('data', [])  # 假设这是响应中包含集合数据的字段
            all_collections.extend(collections)
            # 检查是否还有更多页
            total = response.get('total', 0)  # 假设这是响应中表示总项目数的字段
            if page_size * page_num >= total:
                break  # 已获取所有页，跳出循环
            page_num += 1 

        return all_collections

    async def delete_for_collection(self, id):
        if id:
            url = f'/api/core/dataset/data/delete?id={id}'
            return await self.make_request_async('DELETE', url)
        else:
            raise ValueError("ID is required for deletion.")
    
    async def create_collection(self, dataset_id, name, type="virtual", parent_id=None):
        url = f'/api/core/dataset/collection/create'
        payload = {
            "datasetId":dataset_id,
            "parentId": parent_id,
            "name":name,
            "type":type
        }
        return await self.make_request_async('POST', url, payload)
    
    async def delete_collection(self, id):
        if id:
            url = f'/api/core/dataset/collection/delete?id={id}'
            return await self.make_request_async('DELETE', url)
        else:
            raise ValueError("ID is required for deletion.")


    async def async_search(self, dataset_id, text, limit=10, similarity=0, search_mode="embedding", using_re_rank=False):
        """
        search_for_kb 从知识库中搜索测试

        Args:
            dataset_id: 知识库id
            text: 文本内容
            limit: 限制 默认 10
            similarity: 相似度 默认 0
            search_mode: 搜索模式 "embedding".

        Returns:    
            _description_
        """
        payload = {
            "datasetId": dataset_id,
            "text": text,
            "limit": limit,
            "similarity": similarity,
            "searchMode": search_mode,
            "usingReRank": using_re_rank
        }
        return await self.make_request_async('POST', '/api/core/dataset/searchTest', payload)
    
    def search_for_kb(self, dataset_id, text, limit=10, similarity=0, search_mode="embedding", using_re_rank=False):
        
        payload = {
            "datasetId": dataset_id,
            "text": text,
            "limit": limit,
            "similarity": similarity,
            "searchMode": search_mode,
            "usingReRank": using_re_rank
        }
        return self.make_request_sync('POST', '/api/core/dataset/searchTest', payload)


async def main():
    # 需要根据实际情况替换 'your_base_url' 和 'your_api_key1,your_api_key2'
    lazygpt_client = FastGPTClient('http://gpt.skg.com', fanyi_chat_key)

    # chat调用示例
    # response = await lazygpt_client.chat_completions(messages=[{'content': 'heloo I waht to return', 'role': 'user'}])
    # 知识库中的集合id
    collection_id = "65fbca8d295235b4a4ca96b7"
    # 给集合添加数据
    index = "测试内容的索引"
    data_list = [{
                    'q': "测试",
                    'a': "测试内容",
                    "indexes": [{
                        "defaultIndex": True,
                        "type": "qa",
                        "text": index
                    }],
                }]
    response = await lazygpt_client.async_push_for_collection(collection_id, data_list)
    print(response)
    # 查询集合中的所有数据
    all_for_collection = await lazygpt_client.async_listall_for_collection(collection_id)
    print(all_for_collection)
    # 删除集合中的所有数据
    for item in all_for_collection:
        await lazygpt_client.delete_for_collection(item.get('_id'))


if __name__ == "__main__":
    # 使用示例
    # sk-nFbhU1UkPT95kV1cCUZK73wf3fDLT4052wPKkBKuvVdjybEkQVPURxwEPVsy5iV （判断正负评论问答的key）
    # sk-ticaOBDuCIBynOsU2bU7tgAFYkgBqZtbyB6LUAEvEDXmVCIFMw66Udi7zqmlfIN （知识库公共KEY）
    db_key = "sk-ticaOBDuCIBynOsU2bU7tgAFYkgBqZtbyB6LUAEvEDXmVCIFMw66Udi7zqmlfIN"
    fanyi_chat_key = 'sk-rd2v1zAt3oADuJdpfiCNRFWkV3A33lfDmTtLRi7HdY1X042uMHFvVzReMq5L'
    
    asyncio.run(main())
    # 异步调用示例
    # import asyncio
    # response = asyncio.run(lazygpt_client.chat_completions_async('your_chat_id', [{'content': '导演是谁', 'role': 'user'}]))
