# -*- coding: utf-8 -*-

import uuid
import hashlib

def generate_id(elements):
    # 将所有元素转换为字符串，并通过连接它们来形成一个唯一的字符串
    unique_string = ''.join(map(str, elements))
    # 使用 hashlib 的 md5 方法生成一个哈希对象
    hash_object = hashlib.md5(unique_string.encode())
    # 获取16进制的摘要
    hash_digest = hash_object.hexdigest()
    return hash_digest

def generate_common_id(with_hyphen=False, is_uppercase=False):
    id = str(uuid.uuid4())
    if not with_hyphen:
        id = id.replace('-', '')
    if is_uppercase:
        id = id.upper()
    return id
    
