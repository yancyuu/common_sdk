import asyncio
from common_sdk.system.sys_env import get_env
from common_sdk.service_client.llm.base_client import BaseClient
from typing import Any, Dict, List, Protocol, runtime_checkable
from ._types import WorkflowApiResponse, ChatMessageResponse


# 定义协议类，用于类型检查
@runtime_checkable
class APIClientProtocol(Protocol):
    async def chat_completions(self, query: str, user: str, conversation_id: str = None, inputs: Dict[str, Any] = None,
                               files: List[Dict[str, Any]] = None, auto_generate_name: bool = True) -> Any:
        ...


class DOIT4SELFClient(BaseClient, APIClientProtocol):

    def __init__(self, api_key):
        super().__init__(api_key, get_env("DOIT4SELF_BASE_URL"))

    async def chat_completions(self, query, user, conversation_id=None, inputs=None, files=None,
                               auto_generate_name=True) -> Any:
        """
        chat_completions 对话型应用的对话补全接口

        Args:
            query: 用户输入的对话内容
            user: 用户名称
            conversation_id: _description_. Defaults to None.
            inputs: _description_. Defaults to None.
            files: _description_. Defaults to None.
            auto_generate_name: _description_. Defaults to True.

        Returns:
            _description_
        """
        if files is None:
            files = []
        url = '/chat-messages'
        payload = {
            "query": query,
            "user": user,
            "response_mode": 'blocking',
            "auto_generate_name": auto_generate_name,
            "files": files,
            "inputs": inputs
        }
        if conversation_id:
            payload.update({
                "conversation_id": conversation_id,
            })

        response_data = await self.make_request_async('POST', url, body=payload)
        return ChatMessageResponse(**response_data)

    async def chat_completions_with_streaming(self, query, user, conversation_id=None, inputs=None, files=None,
                                              auto_generate_name=True) -> Any:
        """
        chat_completions 对话型应用的对话补全接口

        Args:
            query: 用户输入的对话内容
            user: 用户名称
            conversation_id: _description_. Defaults to None.
            inputs: _description_. Defaults to None.
            files: _description_. Defaults to None.
            auto_generate_name: _description_. Defaults to True.

        Returns:
            _description_
        """
        if files is None:
            files = []
        if inputs is None:
            inputs = {}
        url = '/chat-messages'
        payload = {
            "query": query,
            "user": user,
            "response_mode": 'streaming',
            "auto_generate_name": auto_generate_name,
            "files": files,
            "inputs": inputs
        }
        if conversation_id:
            payload.update({
                "conversation_id": conversation_id,
            })

        async for chunk in self.make_request_stream_async('POST', url, body=payload):
            yield chunk

    async def chat_completions_stop(self, user, task_id):
        return await self.make_request_async('POST', f'{self.base_url}/chat-messages/{task_id}/stop',
                                             body={"user": user})

    async def completion_messages(self, user: str, conversation_id: str = None, inputs: Dict[str, Any] = None,
                                  files: List[Dict[str, Any]] = None) -> Any:
        """
        completion_messages 文本生成型应用的消息发送接口

        Args:
            user: 用户标识
            conversation_id: （选填）会话 ID，继续之前的对话需传入之前的 conversation_id。
            inputs: 包含各变量值的字典，必须包含键 "query" 对应用户输入的文本内容。
            files: （选填）上传的文件列表。

        Returns:
            返回完整的回复内容。
        """
        url = '/completion-messages'
        if inputs is None or "query" not in inputs:
            raise ValueError("inputs must be provided and must include a 'query' field")

        payload = {
            "inputs": inputs,
            "user": user,
            "response_mode": 'blocking',
        }
        if conversation_id:
            payload["conversation_id"] = conversation_id
        if files:
            payload["files"] = files

        return await self.make_request_async('POST', url, body=payload)

    async def completion_messages_with_streaming(self, user: str, conversation_id: str = None,
                                                 inputs: Dict[str, Any] = None,
                                                 files: List[Dict[str, Any]] = None) -> Any:
        """
        completion_messages 文本生成型应用的消息发送接口

        Args:
            user: 用户标识
            conversation_id: （选填）会话 ID，继续之前的对话需传入之前的 conversation_id。
            inputs: 包含各变量值的字典，必须包含键 "query" 对应用户输入的文本内容。
            files: （选填）上传的文件列表。

        Returns:
            返回流式块。
        """
        url = '/completion-messages'
        if inputs is None or "query" not in inputs:
            raise ValueError("inputs must be provided and must include a 'query' field")

        payload = {
            "inputs": inputs,
            "user": user,
            "response_mode": 'streaming',
        }
        if conversation_id:
            payload["conversation_id"] = conversation_id
        if files:
            payload["files"] = files

        async for chunk in self.make_request_stream_async('POST', url, body=payload):
            yield chunk

    async def completion_messages_stop(self, user, task_id):
        url = f'{self.base_url}/completion-messages/{task_id}/stop'
        return await self.make_request_async('POST', url, body={"user": user})

    async def run_workflow(self, inputs: dict, user: str, files: List[Dict] = []) -> WorkflowApiResponse:
        """
        run_workflow 执行 workflow 的接口

        Args:
            inputs: 传入的各变量值
            user: 用户标识
            response_mode: 返回响应模式，支持 'streaming' 和 'blocking'
            files: 文件列表，适用于传入文件（图片）

        Returns:
            返回执行结果或流式响应
        """
        url = '/workflows/run'
        payload = {
            "inputs": inputs,
            "response_mode": 'blocking',
            "user": user,
            "files": files
        }

        response_data = await self.make_request_async('POST', url, body=payload)

        return WorkflowApiResponse(**response_data)

    async def run_workflow_with_streaming(self, inputs: dict, user: str, files: List[Dict] = []) -> Any:
        """
        run_workflow 执行 workflow 的接口

        Args:
            inputs: 传入的各变量值
            user: 用户标识
            response_mode: 返回响应模式，支持 'streaming' 和 'blocking'
            files: 文件列表，适用于传入文件（图片）

        Returns:
            返回执行结果或流式响应
        """
        url = '/workflows/run'
        payload = {
            "inputs": inputs,
            "response_mode": 'streaming',
            "user": user,
            "files": files
        }

        async for chunk in self.make_request_stream_async('POST', url, body=payload):
            yield chunk

    async def stop_workflow(self, user: str, task_id: str) -> Any:
        """
        stop_workflow 停止 workflow 的接口，仅支持流式模式

        Args:
            user: 用户标识
            task_id: 任务 ID

        Returns:
            返回停止结果
        """
        url = f'/workflows/{task_id}/stop'
        payload = {"user": user}

        return await self.make_request_async('POST', url, body=payload)

    async def feedback_message(self, message_id, rating, user):
        url = f'/messages/{message_id}/feedbacks'
        payload = {
            "rating": rating,
            "user": user
        }
        return await self.make_request_async('POST', url, body=payload)

    async def get_suggested_questions(self, message_id, user):
        url = f'/messages/{message_id}/suggested'
        params = {"user": user}
        return await self.make_request_async('GET', url, params=params)

    async def get_conversation_history(self, conversation_id, user, first_id=None, limit=20):
        url = '/messages'
        params = {
            "conversation_id": conversation_id,
            "user": user,
            "first_id": first_id,
            "limit": limit
        }
        return await self.make_request_async('GET', url, params=params)

    async def upload_file(self, file_path, user):
        url = f'{self.base_url}/files/upload'
        with open(file_path, 'rb') as file:
            files = {
                'file': (file_path, file, 'multipart/form-data'),
                'user': (None, user)
            }
        return await self.make_request_async('POST', {url}, files=files)

    async def get_conversations(self, user, last_id=None, limit=20, pinned=None):
        url = '/conversations'
        params = {
            "user": user,
            "last_id": last_id,
            "limit": limit,
        }
        if pinned is not None:
            params["pinned"] = pinned

        return await self.make_request_async('GET', url, params=params)

    async def delete_conversation(self, conversation_id, user):
        url = f'/conversations/{conversation_id}'
        payload = {"user": user}
        return await self.make_request_async('DELETE', url, body=payload)

    async def rename_conversation(self, conversation_id, name, user, auto_generate=False):
        url = f'/conversations/{conversation_id}/name'
        payload = {
            "name": name,
            "user": user,
            "auto_generate": auto_generate
        }
        return await self.make_request_async('POST', url, body=payload)

    async def audio_to_text(self, file_path, user):
        url = '/audio-to-text'
        files = {
            'file': open(file_path, 'rb'),
            'user': (None, user)
        }
        return await self.make_request_async('POST', url, files=files)

    async def text_to_audio(self, text, user, streaming=False):
        url = '/text-to-audio'
        payload = {
            'text': text,
            'user': user,
            'streaming': str(streaming).lower()
        }
        return await self.make_request_async('POST', url, body=payload)

    async def get_parameters(self, user):
        url = '/parameters'
        params = {"user": user}
        return await self.make_request_async('GET', url, params=params)

    async def get_meta(self, user):
        url = '/meta'
        params = {"user": user}
        return await self.make_request_async('GET', url, params=params)




