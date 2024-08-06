import lark_oapi as lark
from lark_oapi.api.authen.v1 import *
from common_sdk.system.sys_env import get_env
from common_sdk.logging.logger import logger

class Oauth2Client:
    
    def __init__(self, enable_token: bool = False, log_level: lark.LogLevel = lark.LogLevel.INFO):
        self.app_id = get_env("BITTABLE_APP_ID")
        self.app_secret = get_env("BITTABLE_APP_SECRET")
        self.enable_token = enable_token
        self.log_level = log_level
        self.client = self._create_client()

    def _create_client(self):
        builder = lark.Client.builder().app_id(self.app_id).app_secret(self.app_secret)
        if self.enable_token:
            builder.enable_set_token(True)
        builder.log_level(self.log_level)
        return builder.build()

    async def get_access_token(self, code, grant_type="authorization_code"):
        request = CreateOidcAccessTokenRequest.builder() \
            .request_body(CreateOidcAccessTokenRequestBody.builder()
                .grant_type(grant_type)
                .code(code)
                .build()) \
            .build()

        response = await self.client.authen.v1.oidc_access_token.acreate(request)
        return self._process_response(response)

    async def get_user_info(self, access_token):
        # 构造请求对象
        request: GetUserInfoRequest = GetUserInfoRequest.builder() \
            .build()
        option = lark.RequestOption.builder().user_access_token(access_token).build()
        response = await self.client.authen.v1.user_info.aget(request, option)
        return self._process_response(response)

    async def refresh_token(self, refresh_token):
        request = CreateOidcRefreshAccessTokenRequest.builder() \
            .request_body(CreateOidcRefreshAccessTokenRequestBody.builder()
                .grant_type("refresh_token")
                .refresh_token(refresh_token)
                .build()) \
            .build()
        
        response = await self.client.authen.v1.oidc_refresh_access_token.acreate(request)
        return self._process_response(response)

    def _process_response(self, response):
        if not response.success():
            lark.logger.error(
                f"Request failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}")
            return None
        lark.logger.info(lark.JSON.marshal(response.data, indent=4))
        return response.data

# 使用实例
if __name__ == "__main__":
    # 使用你的APP_ID和APP_SECRET初始化客户端
    lark_client = Oauth2Client(enable_token=True)

    lark_client.get_access_token("your_code")

    

