from common_sdk.util.id_generator import generate_common_id

import requests
import httpx
import json
from ..logging.logger import logger
from ..system.sys_env import get_env

class MessageTypes:
    FILE = 1
    VOICE = 2
    EMOTION = 5
    IMAGE = 6
    TEXT = 7
    LOCATION = 8
    MINI_PROGRAM = 9
    LINK = 12
    VIDEO = 13
    VIDEO_NUMBER = 14

class MessagePayloadBuilder:
    '''用于封装payload'''
    def __init__(self):
        self.message_type = None
        self.payload = None
    
    def init_file_message(self, name, url, size=None):
        self.message_type = MessageTypes.FILE
        self.payload = {
            "name": name,
            "url": url,
            "size": size
        }

    def init_voice_message(self, voice_url, duration=None):
        self.message_type = MessageTypes.VOICE
        self.payload = {
            "voiceUrl": voice_url,
            "duration": duration
        }
    
    def init_pic_message(self, pic_url, size=None):
        self.message_type = MessageTypes.IMAGE
        self.payload = {
            "url": pic_url,
            "size": size
        }

    def init_text_message(self, text, mention=[]):
        self.message_type = MessageTypes.TEXT
        self.payload = {
            "text": text,
            "mention": mention
        }

class JuziClient:
    '''有赞的句子插件接入机器人自动回复'''
    def __init__(self):
        self.base_url = get_env("JUZI_BASE_URL")
        self.token = get_env("JUZI_TOKEN")
        self.headers = {
            'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
            'Content-Type': 'application/json'
        }

    def create_message_data(self, external_request_id, im_bot_id, im_contact_id, im_room_id, payload_builder: MessagePayloadBuilder):
        common_data = {
            "externalRequestId": external_request_id,
            "imBotId": im_bot_id,
            "messageType": payload_builder.message_type,
            "payload": payload_builder.payload
        } 
        if im_contact_id:
            common_data.update({"imContactId": im_contact_id})
        if im_room_id:
            common_data.update({"imRoomId": im_room_id})
        return common_data    
    
    async def asend_message(self, im_bot_id, im_contact_id, im_room_id, payload_builder: MessagePayloadBuilder):
        message_data = self.create_message_data(
            external_request_id=generate_common_id(),
            im_bot_id=im_bot_id,
            im_contact_id=im_contact_id,
            im_room_id=im_room_id,
            payload_builder=payload_builder
        )
        endpoint = f"/api/v2/message/send?token={self.token}"
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.base_url}{endpoint}", headers=self.headers, json=message_data)
        return self.parse_response(response)

    def send_message(self, im_bot_id, im_contact_id, im_room_id, payload_builder: MessagePayloadBuilder):
        # 构造消息数据
        message_data = self.create_message_data(
            external_request_id=generate_common_id(),
            im_bot_id=im_bot_id,
            im_contact_id=im_contact_id,
            im_room_id=im_room_id,
            payload_builder=payload_builder
        )
        endpoint = f"/api/v2/message/send?token={self.token}"
        logger.info(f"发送消息到句子 url={self.base_url}{endpoint} message_data={message_data}")
        response = requests.post(f"{self.base_url}{endpoint}", headers=self.headers, json=message_data)
        return self.parse_response(response)
    
    def get_user_detail(self, wx_user_id):
        # 构造请求URL
        endpoint = f"/api/v1/user/detail?token={self.token}&wxUserId={wx_user_id}"
        # 发起GET请求
        response = requests.post(f"{self.base_url}{endpoint}", headers=self.headers, data=json.dumps(message_data))
        # 返回响应数据
        return self.parse_response(response)

    def get_bot_id_by_user(self, wecom_user_id, corp_id=None):
        endpoint = f"/api/v1/bot/wecomUserId_to_botId?token={self.token}&wecomUserId={wecom_user_id}"
        if corp_id:
            endpoint += f"&corpId={corp_id}"
        full_url = f"{self.base_url}{endpoint}"

        response = requests.get(full_url, headers=self.headers)
        return self.parse_response(response, "botId")

    
    def get_all_users_with_bot_id(self, current=1, pageSize=10):
        user_list = []

        total = None
        # 分页查询所有用户
        while total is None or len(user_list) < total:
            # 构造请求URL，包括查询参数
            endpoint = f"/api/v1/user/list?token={self.token}&current={current}&pageSize={pageSize}"
            full_url = f"{self.base_url}{endpoint}"
            # 发起GET请求
            logger.info(f"发送到句子的查询用户列表的GET请求 url={full_url}")
            user_list_response = requests.get(full_url, headers=self.headers)
            if user_list_response.status_code != 200:
                logger.info(f"句子请求用户细节失败{user_list_response}")
                return {}
            response_data = user_list_response.json()
            if response_data.get('errcode') != 0:
                logger.info(f"句子请求用户细节失败{response_data}")
                return {}
            logger.info(f"句子请求用户列表成功: {response_data}")
            if response_data is None:
                break  # 如果获取用户列表失败，则终止循环

            users_data = response_data.get('data', [])
            if not users_data:  # 如果当前页没有用户数据，停止循环
                break

            if total is None:
                total = response_data.get('total', 0)
            
            for user in response_data.get('data', []):
                user_list.append(user)  # 使用 botId 作为键，存储用户信息
            current += 1

        return user_list

    
    def parse_response(self, response, datakey = "data"):
        # 根据响应状态码处理响应数据
        if response.status_code == 200:
            response_data = response.json()
            if response_data.get('errcode') == 0:
                return response_data.get(datakey)
            else:
                logger.error(f"Failed to get from juzi: {response_data}")
                return
        else:
            logger.error(f"HTTP Error: {response.status_code}")
        return


juzi_client = JuziClient()

if __name__ == "__main__":
    # 使用示例
    client = JuziClient()
    # 发送消息 - 同步
    message_data= {}
    print(client.send_message(message_data))

    # 发送消息 - 异步
    import asyncio

    async def main():
        print(await client.asend_message(message_data))

    asyncio.run(main())