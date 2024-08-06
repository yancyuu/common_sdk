import base64
import hashlib
import requests
from cryptography.hazmat.primitives.ciphers import  algorithms, Cipher, modes
from cryptography.hazmat.backends import default_backend
from ..system.sys_env import get_env
from ..logging.logger import logger
from service.errors import error_codes, Error
import json
import time


class JingyuClient:
    # 消息类型
    CONTENT_TYPE = 1
    # 图片
    PIC_TYPE = 3
    # 视频
    VIDIO_TYPE = 4

    def __init__(self):
        self.encoding_aes_key = get_env("JINGYU_ENCODING_AES_KEY")
        self.app_key = get_env("JINGYU_APP_KEY")
        self.app_secret = get_env("JINGYU_APP_SECRET")
        self.token = get_env("JINGYU_TOKEN")
        self.url = get_env("JINGYU_BASE_URL")

    def strip_pkcs5_padding(self, data):
        if len(data) == 0:
            return b""
        padding_size = data[-1]
        return data[:-padding_size]

    def decrypt(self, data):
        key = self.encoding_aes_key.encode('utf-8')
        iv_length = 16  # AES block size is 16 bytes
        iv = key[:iv_length]

        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()

        data = base64.b64decode(data)
        decrypted_data = decryptor.update(data) + decryptor.finalize()
        decrypted_data = self.strip_pkcs5_padding(decrypted_data)
        try:
            return decrypted_data.decode('utf-8')
        except UnicodeDecodeError:
            raise Error(error_codes.JINGYU_DATA_CANNT_DECRYPTED)

    # 校验签名
    def verify_signature(self, params, received_signature):
        sort_str = ''.join(sorted([str(params[k]) for k in sorted(params.keys())]))
        m = hashlib.md5()
        m.update(sort_str.encode('utf-8'))
        calc_signature = m.hexdigest()
        return calc_signature == received_signature

    def handle_callback(self, app_key, nonce, timestamp, encoding_content, signature):
        # 验证签名
        if not self.verify_signature({
            'app_key': app_key,
            'token': self.token,
            'nonce': nonce,
            'timestamp': timestamp,
            'encoding_content': encoding_content
        }, signature):
            raise Error(error_codes.JINGYU_AUTHORIZATION_FAILED)

        # 解密内容
        return self.decrypt(encoding_content)
    
    def get_access_token(self):
        url = f"{self.url}/gateway/qopen/GetAccessToken"
        payload = {
            "app_key": self.app_key,
            "app_secret": self.app_secret
        }
        headers = {'Content-Type': 'application/json;charset=UTF-8'}
        logger.info(f"get_access_token: request param {payload}  headers{headers} timestamp {time.time()}")
        response = requests.post(url, headers=headers, json=payload)
        response_data = response.json()
        if response_data.get('errcode') == 0:
            self.access_token = response_data.get('data',{}).get('data',{}).get('access_token')
            return self.access_token
        else:
            logger.info(f"Failed to get access token: {response_data.get('errmsg')}")
            return
    
    def send_message_to_account(self, robot_id, account_id, msg_id, msg_list=[], deadline=None):
        '''发送文本消息'''
        if not msg_list:
            return
        # 先获取token
        token = self.get_access_token()
        if not token:
            raise Error(error_codes.JINGYU_GET_TOKEN_ERROR)
        url = f"{self.url}/gateway/qopen/SendMessageToAccount"
        headers = {
            'Content-Type': 'application/json; charset=UTF-8',
            'Token': token
        }
        payload = {
            "robot_id": robot_id,
            "account_id": account_id,
            "msg_id": msg_id,
        }
        if deadline:
            payload['dead_line'] = deadline
        payload['msg_list'] = msg_list
        print(f"Payload: {json.dumps(payload, indent=4)}")  # 打印payload
        response = requests.post(url, headers=headers, data=json.dumps(payload))  # 使用data而不是json
        logger.info(f"create request by jingyu: url {url} headers{headers} json {payload}")
        return self.parse_response(response)
    
    def get_host_account_list(self, offset=0, limit=10):
        # 先获取token
        token = self.get_access_token()
        if not token:
            raise Error(error_codes.JINGYU_GET_TOKEN_ERROR)
        url = f"{self.url}/gateway/qopen/GetHostAccountList"
        headers = {
            'Content-Type': 'application/json;charset=UTF-8',
            'Token': token
        }
        payload = {
            'offset': offset,
            'limit': limit
        }
        response = requests.post(url, headers=headers, json=payload)
        logger.info(f"create request by jingyu: url {url} headers{headers} json {payload}")
        return self.parse_response(response)

    def get_robot_account_by_ids(self, robot_id, account_id_list, need_tag_info=False):
        # 先获取access token
        token = self.get_access_token()
        if not token:
            raise Error(error_codes.JINGYU_GET_TOKEN_ERROR)
        # 构建URL
        url = f"{self.url}/gateway/qopen/GetRobotAccountByIds"
        # 构建请求头
        headers = {
            'Content-Type': 'application/json; charset=UTF-8',
            'Token': token  # 注意，这里使用的是获取到的access token
        }
        # 构建请求体
        payload = {
            "robot_id": robot_id,
            "account_id_list": account_id_list,
            "need_tag_info": need_tag_info,
            "external_user_id_list":[]
        }
        # 发送请求
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        logger.info(f"create request by jingyu: url {url} headers{headers} json {payload}")
        return self.parse_response(response)

    
    def get_host_account_by_ids(self, host_account_id_list):
        # 确保已获取access token
        token = self.get_access_token()
        if not token:
            raise Error(error_codes.JINGYU_GET_TOKEN_ERROR)
        # 构建请求的URL
        url = f"{self.url}/gateway/qopen/GetHostAccountByIds"
        # 设置请求头
        headers = {
            'Content-Type': 'application/json; charset=UTF-8',
            'Token': token  # 这里使用的是获取到的access token
        }
        # 构建请求体
        payload = {
            "host_account_id_list": host_account_id_list,
        }
        # 发起POST请求
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        logger.info(f"create request by jingyu: url {url} headers{headers} json {payload}")
        return self.parse_response(response)
    
    def parse_response(self, response):
        # 根据响应状态码处理响应数据
        if response.status_code == 200:
            response_data = response.json()
            if response_data.get('errcode') == 0:
                return response_data.get('data')
            else:
                logger.error(f"Failed to get from jingyu: {response_data}")
                return
        else:
            logger.error(f"HTTP Error: {response.status_code}")
        return

jingyu_client = JingyuClient()