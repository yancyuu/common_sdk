# -*- coding: utf-8 -*-

from collections.abc import Sequence

from google.protobuf import json_format


def dict_to_protobuf(protobuf_json, protobuf_cls):
    if protobuf_json is None:
        return
    return json_format.ParseDict(protobuf_json, protobuf_cls(),
                                 ignore_unknown_fields=True)


def batch_dict_to_protobuf(protobuf_json_list, protobuf_cls):
    if protobuf_json_list is None:
        return
    if not isinstance(protobuf_json_list, Sequence):
        raise ValueError('protobuf_json_list of type "{}" is not iterable.'.format(
            type(protobuf_json_list)))
    return [dict_to_protobuf(protobuf_json, protobuf_cls)
            for protobuf_json in protobuf_json_list]


def protobuf_to_dict(protobuf):
    if protobuf is None:
        return None

    # 使用 json_format 将 Protobuf 对象转换为字典
    dict_result = json_format.MessageToDict(
        protobuf,
        preserving_proto_field_name=True,  # 保持字段名与 Protobuf 定义的一致
        use_integers_for_enums=False  # 将枚举值转换为整数
    )

    # 手动检查并确保枚举字段转换为名称字符串
    for field in protobuf.DESCRIPTOR.fields:
        field_name = field.name
        if field_name not in dict_result:
            # 检查字段是否是枚举类型
            if field.enum_type is not None:
                # 获取字段的枚举名称
                enum_value = getattr(protobuf, field_name)
                enum_name = field.enum_type.values_by_number[enum_value].name
                dict_result[field_name] = enum_name
            else:
                # 获取默认值并添加到字典
                default_value = getattr(protobuf, field_name)
                dict_result[field_name] = default_value

    return dict_result


def batch_protobuf_to_dict(protobuf_list):
    if protobuf_list is None:
        return
    if not isinstance(protobuf_list, Sequence):
        raise ValueError('protobuf_list of type "{}" is not iterable.'.format(
            type(protobuf_list)))
    return [protobuf_to_dict(protobuf) for protobuf in protobuf_list]
