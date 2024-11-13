from collections.abc import Sequence
from pydantic import BaseModel
from typing import Type, List, Dict, Union


def dict_to_pydantic(data: Dict, pydantic_cls: Type[BaseModel]) -> BaseModel:
    """
    将字典转换为指定的 Pydantic 模型实例。
    """
    if data is None:
        return None
    return pydantic_cls.parse_obj(data)


def batch_dict_to_pydantic(data_list: List[Dict], pydantic_cls: Type[BaseModel]) -> List[BaseModel]:
    """
    将字典列表批量转换为指定的 Pydantic 模型实例列表。
    """
    if data_list is None:
        return []
    if not isinstance(data_list, Sequence):
        raise ValueError(f'data_list of type "{type(data_list)}" is not iterable.')
    return [dict_to_pydantic(data, pydantic_cls) for data in data_list]


def pydantic_to_dict(model: BaseModel) -> Dict:
    """
    将 Pydantic 模型实例转换为字典。
    """
    if model is None:
        return {}
    return model.model_dump()


def batch_pydantic_to_dict(model_list: List[BaseModel]) -> List[Dict]:
    """
    将 Pydantic 模型实例列表批量转换为字典列表。
    """
    if model_list is None:
        return []
    if not isinstance(model_list, Sequence):
        raise ValueError(f'model_list of type "{type(model_list)}" is not iterable.')
    return [pydantic_to_dict(model) for model in model_list]