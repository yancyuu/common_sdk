import hmac
import hashlib
import base64
import requests
from datetime import datetime
from urllib.parse import urlencode, quote
from common_sdk.system.sys_env import get_env
from common_sdk.logging.logger import logger

class TianrunClient:
    def __init__(self):
        self.access_key_id = get_env("TIANRUN_ACCESS_KEY_ID")
        self.access_key_secret = get_env("TIANRUN_ACCESS_KEY_SECRET")
        self.host = "api-sh.clink.cn"

    def generate_signature(self, method, base_url, params):
        # 添加必要的参数
        params.update({
            'AccessKeyId': self.access_key_id,
            'Expires': 300,  # 过期时间（根据需要调整）
            'Timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        })
        # 对参数进行字典排序
        sorted_params = sorted(params.items())
        
        # 使用 & 符号连接排序后的参数名称和值
        query_string = urlencode(sorted_params, quote_via=quote)

        # 按照规则拼接要加密的字符串
        string_to_sign = method.upper() + base_url + '?' + query_string

        # 使用 HMAC-SHA1 算法对字符串进行加密
        signature = hmac.new(
            self.access_key_secret.encode('utf-8'),
            string_to_sign.encode('utf-8'),
            hashlib.sha1
        ).digest()

        # 对加密后的字符串进行 Base64 编码
        signature_base64 = base64.b64encode(signature).decode('utf-8')

        # 返回最终的签名结果
        return signature_base64
    
    def list_directories(self, name=None, type=None):
        method = 'GET'
        base_url = '/kb/list_directories'
        params = {'name': name, 'type': type} if name or type else {}
        # 生成签名
        signature = self.generate_signature(method, self.host + base_url, params)
        params['Signature'] = signature
        logger.info(f"params -------> {params}")
        response = requests.get(f'https://{self.host}{base_url}?{urlencode(params)}')
        return response.json()
    
    def create_article(self, article_data):
        method = 'POST'
        base_url = '/kb/create_article'
        params = {}
        signature = self.generate_signature(method, self.host + base_url, params)
        params['Signature'] = signature
        response = requests.post(f'https://{self.host}{base_url}?{urlencode(params)}', json=article_data)
        return response.json()

    def update_article(self, article_data):
        method = 'POST'
        base_url = '/kb/update_article'
        params = {}
        signature = self.generate_signature(method, self.host + base_url, params)
        params['Signature'] = signature
        response = requests.post(f'https://{self.host}{base_url}?{urlencode(params)}', json=article_data)
        return response.json()
    
    def list_articles(self, kbid=None, directory_id=None, page=1, size=100, kb_type=None):
        method = 'GET'
        base_url = '/kb/list_articles'
        params = {
            'kbId': kbid,
            'kbType': kb_type,
            'directoryId': directory_id,
            'offset': (page-1) * size,
            'limit': size
        }
        # 生成签名
        signature = self.generate_signature(method, self.host + base_url, params)
        params['Signature'] = signature
        response = requests.get(f'https://{self.host}{base_url}?{urlencode(params)}')
        return response.json()


    
tianrun_client = TianrunClient()


