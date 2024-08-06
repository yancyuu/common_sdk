# -*- coding: utf-8 -*-

from contextvars import ContextVar

HTTP_MESSAGE_UUID = "message_id"
HTTP_USER_IP = "user_ip"
HTTP_REQUEST_TIMESTAMP = "request_timestamp"
COMMON_CONTEXT = 'context'

_message_id_ctx_var: ContextVar[str] = ContextVar(HTTP_MESSAGE_UUID, default=None)
_user_ip_ctx_var: ContextVar[str] = ContextVar(HTTP_USER_IP, default="0.0.0.0")
_request_timestamp_ctx_var: ContextVar[int] = ContextVar(HTTP_REQUEST_TIMESTAMP, default=None)
_common_ctx_var: ContextVar[dict] = ContextVar(COMMON_CONTEXT, default={})


def get(key: str):
    return _common_ctx_var.get().get(key)


def set(key: str, val) -> None:
    _common_ctx_var.get().update({key: val})


def get_message_uuid() -> str:
    return _message_id_ctx_var.get()


def set_message_uuid(id: str) -> str:
    return _message_id_ctx_var.set(id)

def get_user_ip() -> str:
    return _user_ip_ctx_var.get()


def set_user_ip(ip: str) -> str:
    return _user_ip_ctx_var.set(ip)

def get_request_timestamp() -> int:
    return _request_timestamp_ctx_var.get()


def set_request_timestamp(timestamp: int) -> int:
    return _request_timestamp_ctx_var.set(timestamp)