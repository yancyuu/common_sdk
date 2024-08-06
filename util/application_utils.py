# -*- coding: utf-8 -*-
import os


def get_secret(app_id):
    if app_id == os.environ.get("WECHAT_HELPER_MINI_APP_ID"):
        return os.environ.get("WECHAT_HELPER_MINI_APP_SECRET")
    if app_id == os.environ.get("WECHAT_SHILAI_SHIHUI_APP_ID"):
        return os.environ.get("WECHAT_SHILAI_SHIHUI_APP_SECRET")
    if app_id == os.environ.get("WECOM_CORPID"):
        return os.environ.get("WECOM_SECRET_SHILAISHANGHUZHUSHOU")
        # todo:增加其余情况
    return
