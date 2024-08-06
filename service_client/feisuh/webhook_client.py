import base64
import hmac
import hashlib
import requests
import time


class WebhookClient:
    
    def __init__(self, webhook_url, secret=None):
        self.secret = secret
        self.webhook_url = webhook_url

    def _get_timestamp_and_sign(self):
        timestamp = str(int(time.time()))
        print(f"timestamp {timestamp}")
        # 拼接签名字符串
        string_to_sign = '{}\n{}'.format(timestamp, self.secret)
        # 使用HMAC SHA256算法计算签名
        hmac_code = hmac.new(self.secret.encode("utf-8"), string_to_sign.encode("utf-8"), digestmod=hashlib.sha256).digest()
        
        # 对结果进行Base64编码
        sign = base64.b64encode(hmac_code).decode('utf-8')
        
        return timestamp, sign
    
    def send_message(self, msg_type, content):
        payload = {}
        if self.secret:
            timestamp, sign = self._get_timestamp_and_sign()
            payload.update({
                "timestamp": timestamp,
                "sign":sign
            })
        payload.update({
            "msg_type": msg_type,
            "content": content
        })
        print(f"payload {payload}")
        headers = {"Content-Type": "application/json"}
        response = requests.post(self.webhook_url, headers=headers, json=payload)
        return response.json()  # 返回响应内容



if __name__ == '__main__':

    webhook_url = 'https://open.feishu.cn/open-apis/bot/v2/hook/3e0d440f-265f-4deb-96b0-513c4c6ea8d0' # 你的 Webhook URL
    bot = WebhookClient(webhook_url)
    content= {
                "post": {
                    "zh_cn": {
                        "title": "这是一条测试数据，无需回复",
                        "content": [
                            [{
                                    "tag": "text",
                                    "text": "客服工作台: "
                            },
                            {
                                    "tag": "a",
                                    "text": "请查看",
                                    "href": "https://www.xunjinet.com.cn/app/quan-msgv2/"
                            },
                            {
                                    "tag": "at",
                                    "user_id": "ou_a1950b590dd860704540f503f696be44"
                            }
                            ]
                            ]
                        }
                    }
            }
    response = bot.send_message("post", content)