#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 火山引擎录音文件识别标准版，https://www.volcengine.com/docs/6561/80820
import time
import aiohttp
from common_sdk.system.sys_env import get_env

app_id = get_env('HS_APP_ID')
access_token = get_env('HS_ACCESS_TOKEN')
secret_key = get_env('HS_SECRET_KEY')


class HuoshanClient(object):
    def __init__(self):
        self.api_key = app_id
        self.access_token = access_token
        self.secret_key = secret_key
        self.cluster = "volc_auc_common"
        self.host = "https://openspeech.bytedance.com/api/v1/auc"

    async def request_data(self, url, data):
        headers = {"Content-Type": "application/json",
                   "Authorization": f"Bearer; {access_token}"}
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=data, headers=headers) as response:
                if response.status == 200:
                    print(f"[LLM] response {await response.text()}")
                    return await response.json()
                else:
                    response.raise_for_status()

    async def create_task(self, audio_url, audio_format):
        data = {
            "app": {
                "appid": app_id,
                "token": access_token,
                "cluster": self.cluster
            },
            "user": {
                "uid": "juewei_tts"
            },
            "audio": {
                "format": audio_format,
                "url": audio_url
            },
            "additions": {
                "use_itn": "False",
                "with_speaker_info": "True"
            },
        }
        res = await self.request_data(self.host+"/submit", data)
        if res["resp"]["message"] == "success":
            return res["resp"]["id"]
        return None

    async def query_task(self, task_id):
        text = None
        data = {
            "appid": app_id,
            "token": access_token,
            "cluster": self.cluster,
            "id": task_id
        }
        for i in range(100):
            print("time sleep")
            time.sleep(3)
            res = await self.request_data(self.host + "/query", data)
            if res['resp']['code'] == 1000:  # task finished
                print("识别成功")
                return res["resp"]["text"]
            elif res['resp']['code'] < 2000:  # task failed
                print("识别失败")
                break
        return None

    async def run_tts(self, audio_url, audio_format):
        task_id = await self.create_task(audio_url, audio_format)
        print(f"生产识别任务id：{task_id}")
        if task_id:
            result = await self.query_task(task_id)
            print("result ", result)
            return {"result": result}
        return None


if __name__ == '__main__':
    import asyncio
    asyncio.run(
        HuoshanClient().run_tts("https://sf5-hl-cdn-tos.douyinstatic.com/obj/ies-music/7404242672401074970.mp3", "mp3")
    )


