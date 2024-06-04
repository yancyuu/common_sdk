import httpx
import asyncio
from common_sdk.system.sys_env import get_env

class AsyncOauth2Client:

    def __init__(self):
        self.client_id = get_env("IAM_CLIENT_ID")
        self.client_secret = get_env("IAM_CLIENT_SECRET")
        self.redirect_uri = get_env("IAM_REDIRECT_URL")
        self.base_url = get_env("IAM_BASE_URL")    
    
    async def get_access_token(self, code, grant_type='authorization_code'):
        payload = {
            'grant_type': grant_type,
            'code': code,
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'redirect_uri': self.redirect_uri
        }
        async with httpx.AsyncClient() as client:
            response = await client.post(self.base_url+"/oauth/token", data=payload)
            return response.json()

    async def get_user_info(self, access_token):
        userinfo_url = f"{self.base_url}/api/bff/v1.2/oauth2/userinfo?access_token={access_token}"
        async with httpx.AsyncClient() as client:
            response = await client.get(userinfo_url)
            return response.json()

