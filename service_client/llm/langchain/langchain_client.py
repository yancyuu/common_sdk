from langchain_openai import AzureChatOpenAI
from common_sdk.system.sys_env import get_env

class AzureChatOpenAIClient:

    def __init__(self):
        self.llm = AzureChatOpenAI(
            deployment_name=get_env("AZURE_OPENAI_DEPLOYMENT_NAME"),
            api_key=get_env("AZURE_OPENAI_API_KEY"),
            api_version=get_env("AZURE_OPENAI_API_VERSION"),
            azure_endpoint=get_env("AZURE_OPENAI_ENDPOINT"),
            model="gpt-4o",
            temperature=0.7,
        )

    def get_llm(self):
        return self.llm

