import json
from sqlalchemy.ext.declarative import DeclarativeMeta
from decimal import Decimal
from datetime import datetime


class AlchemyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj.__class__, DeclarativeMeta):
            fields = {}
            for field in [x for x in dir(obj) if not x.startswith('_') and x != 'metadata']:
                data = obj.__getattribute__(field)

                # 排除 registry 等 SQLAlchemy 内部属性
                if field == 'registry' or isinstance(data, DeclarativeMeta):
                    continue

                if isinstance(data, Decimal):
                    fields[field] = float(data)
                elif isinstance(data, datetime):
                    fields[field] = data.isoformat()
                elif isinstance(data, list):
                    fields[field] = [self.default(item) for item in data]
                else:
                    try:
                        json.dumps(data)  # 试图序列化非默认可序列化对象
                        fields[field] = data
                    except TypeError:
                        fields[field] = str(data)  # 转换为字符串以避免序列化错误
            return fields
        return super().default(obj)


def alchemy_to_dict(data):
    if isinstance(data.__class__, DeclarativeMeta):
        encoder = AlchemyEncoder()
        return encoder.default(data)
    else:
        raise TypeError("Provided object is not a SQLAlchemy object")
