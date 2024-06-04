import requests
import asyncio
import httpx


class WebhookClient:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url

    def send_message(self, message_type, content):
        """ 同步发送消息 """
        headers = {'Content-Type': 'application/json'}
        data = {
            "msgtype": message_type,
            message_type: {
                "content": content
            }
        }
        response = requests.post(self.webhook_url, headers=headers, json=data)
        return response.status_code, response.json()


    async def send_message_async(self, message_type, content):
        """ 使用httpx异步发送消息 """
        headers = {'Content-Type': 'application/json'}
        data = {
            "msgtype": message_type,
            message_type: {
                "content": content
            }
        }
        async with httpx.AsyncClient() as client:
            response = await client.post(self.webhook_url, headers=headers, json=data)
            return response.status_code, response.json()

if __name__ == "__main__":
    # 使用示例
    webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=0e9baeeb-9692-4f64-9dd6-98c5a0e2335d"
    client = WebhookClient(webhook_url)

    # 同步发送
    status, response = client.send_message("text", "hello world")
    print(status, response)

    # 异步发送
    async def main():
        status, response = await client.send_message_async("text", "hello world")
        print(status, response)

    asyncio.run(main())
