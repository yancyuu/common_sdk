# -*- coding: utf-8 -*-

import hashlib
from io import BytesIO
import qrcode
import uuid


def create_qrcode(content):
    """ 创建二维码"""
    img = qrcode.make(content)
    save_quality = 95
    query_sum = 0
    size = 512000
    while True:
        img_byte_arr = BytesIO()
        query_sum += 1
        img.save(img_byte_arr, format='PNG', quality=save_quality)
        pic_size_bytes = img_byte_arr.tell()
        if pic_size_bytes < size:
            break
        if isinstance(save_quality, int) and query_sum < 90:
            save_quality = int(save_quality) - int(query_sum)
            continue
        else:
            break
    return img_byte_arr.getvalue()


def generate_qrcode_id(content=None):
    # 若二维码内容未提供，则默认生成新uuid
    if not content:
        return str(uuid.uuid4()).replace('-', '')
    else:  # 若提供二维码内容，则根据内容生成哈希值作为ID
        return hashlib.md5(content.encode(encoding='UTF-8')).hexdigest()
