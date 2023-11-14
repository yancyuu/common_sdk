# -*- coding: utf-8 -*-

import datetime
from ipaddress import IPv4Address
import json
from typing import Any
import ujson


class UJSONEncoder(json.JSONEncoder):

    def default(self, o: Any) -> Any:
        try:
            if isinstance(o, datetime.datetime):
                return o.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(o, datetime.date):
                return o.strftime('%Y-%m-%d')
            elif isinstance(o, IPv4Address):
                return str(o)
            return ujson.dumps(o)
        except TypeError:
            return json.JSONEncoder.default(self, o)

    def encode(self, o: Any) -> str:
        try:
            return ujson.encode(o)
        except TypeError:
            return super().encode(o)


def monkey_patch_json() -> None:
    json.__name__ = 'ujson'
    # json.dumps = ujson.dumps
    # json.loads = ujson.loads
    json._default_encoder = UJSONEncoder(
        skipkeys=False,
        ensure_ascii=False,
        check_circular=True,
        allow_nan=True,
        indent=None,
        separators=None,
        default=None,
    )
