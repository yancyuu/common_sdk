import os
import backoff
import httpx
from typing import Any, Dict, List
from common_sdk.service_client.llm.base_client import BaseClient
from openai import AzureOpenAI, APIError

"""待优化：模仿dify写法"""


class AzureOpenAIClient(BaseClient):

    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        self.api_key = os.getenv("AZURE_OPENAI_API_KEY")
        self.api_version = os.getenv("AZURE_OPENAI_API_VERSION")

        self.client = AzureOpenAI(
            api_key=self.api_key,
            azure_endpoint=self.endpoint,
            api_version=self.api_version,
        )

    @backoff.on_exception(
        backoff.expo,
        (httpx.HTTPStatusError, httpx.TimeoutException),
        max_tries=5,
        giveup=lambda e: e.response is not None and e.response.status_code < 500
    )
    async def chat_completion(self, model, messages: List[Dict[str, Any]]) -> str:
        try:
            response = await self.client.chat.completions.create(
                model=model,
                messages=messages
            )
            return response.choices[0].message.content
        except Exception as e:
            raise e
