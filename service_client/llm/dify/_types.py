from __future__ import annotations
from pydantic import BaseModel
from typing import List, Dict, Optional, Literal


class UsageData(BaseModel):
    prompt_tokens: int
    prompt_unit_price: str
    prompt_price_unit: str
    prompt_price: str
    completion_tokens: int
    completion_unit_price: str
    completion_price_unit: str
    completion_price: str
    total_tokens: int
    total_price: str
    currency: str
    latency: float


class RetrieverResource(BaseModel):
    position: int
    dataset_id: str
    dataset_name: str
    document_id: str
    document_name: str
    segment_id: str
    score: float
    content: str


class Metadata(BaseModel):
    usage: UsageData
    retriever_resources: Optional[List[RetrieverResource]] = None


class WorkflowData(BaseModel):
    id: str
    workflow_id: str
    status: str
    outputs: Optional[Dict[str, str]]
    error: Optional[str]
    elapsed_time: float
    total_tokens: int
    total_steps: int
    created_at: int
    finished_at: int


">>>>>>>>>>>>>>"


class WorkflowApiResponse(BaseModel):
    task_id: str
    """任务的唯一标识符"""

    workflow_run_id: str
    """工作流运行的唯一标识符"""

    data: WorkflowData
    """包含工作流运行的详细信息"""


class ChatMessageResponse(BaseModel):
    event: Literal["message"]
    message_id: str
    conversation_id: str
    mode: str
    answer: str
    metadata: Metadata
    created_at: int
