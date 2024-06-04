# -*- coding: utf-8 -*-

import uuid


def generate_common_id(with_hyphen=False, is_uppercase=False):
    id = str(uuid.uuid4())
    if not with_hyphen:
        id = id.replace('-', '')
    if is_uppercase:
        id = id.upper()
    return id
    
import hashlib

