import asyncio
from tencentcloud.common import credential
from tencentcloud.sms.v20210111 import sms_client, models
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException

from common_sdk.system.sys_env import get_env


class TencentSMSClient:
    def __init__(self):
        secret_id = get_env('TENCENT_SMS_SECRET_ID')
        secret_key = get_env('TENCENT_SMS_SECRET_KEY')
        region = get_env('TENCENT_SMS_REGION')
        sms_sdk_appid = get_env('TENCENT_SMS_SDK_APP_ID')
        sign_name = get_env('TENCENT_SMS_SIGN_NAME')

        # 创建腾讯云认证对象
        cred = credential.Credential(secret_id, secret_key)

        # 创建 SMS 客户端
        self.client = sms_client.SmsClient(cred, region)

        # 构造请求参数
        self.req = models.SendSmsRequest()
        self.req.SmsSdkAppId = sms_sdk_appid
        self.req.SignName = sign_name

    async def send_sms(self, phone_number, template_id, template_params=None):
        try:
            self.req.TemplateId = template_id
            self.req.PhoneNumberSet = [phone_number]
            if template_params:
                self.req.TemplateParamSet = template_params

            # 异步调用发送短信
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self.client.SendSms, self.req)

            # 返回结果
            return result.to_json_string()
        except TencentCloudSDKException as err:
            return {"error": str(err)}


tencent_sms_client = TencentSMSClient()

