import ijson
from io import BytesIO
from common_sdk.logging.logger import logger
from datetime import datetime


def streaming_to_conversation(buffer):
    # 移动到缓冲区的开始
    buffer.seek(0)
    # 读取所有数据
    data = buffer.read().decode('utf-8').strip()
    # 以 'data:' 分割数据
    json_strings = data.split("data:")
    # 初始化临时字典和数据解析器
    temp = {}
    conversation_data = {}

    for json_str in json_strings:
        if json_str.strip():  # 确保不处理空字符串
            try:
                # 使用 BytesIO 让 ijson 可以从字符串中解析 JSON
                buffer = BytesIO(json_str.encode('utf-8'))
                parser = ijson.parse(buffer)

                for prefix, event, value in parser:
                    # 使用前缀来确定数据的位置和类型
                    conversation_data_event = None
                    if prefix.endswith('event') and value:
                        conversation_data_event = value
                    if prefix.endswith('conversation_id') and value:
                        conversation_data['conversation_id'] = value
                    elif prefix.endswith('message_id') and value:
                        conversation_data['message_id'] = value
                    elif prefix.endswith('task_id') and value:
                        conversation_data['task_id'] = value
                    elif prefix.endswith('answer') and value:
                        conversation_data['answer'] = value
                    elif prefix.endswith('created_at') and value:
                        conversation_data['created_at'] = datetime.fromtimestamp(value)
                    elif prefix.endswith('metadata.usage.total_tokens') and value:
                        conversation_data['total_tokens'] = value
                    elif prefix.endswith('metadata.usage.total_price') and value:
                        conversation_data['total_price'] = value
                    elif prefix.endswith('metadata.usage.currency') and value:
                        conversation_data['currency'] = value
                    # 检测到一个完整的事件结束，更新消息字典
                    if event == 'end_map' and temp.get('event'):
                        if conversation_data_event == 'message':
                            conversation_data['answer'] = conversation_data.get('answer', '') + temp.get('answer', '')
                        elif conversation_data_event == 'message_end' or temp['event'] == 'error':
                            conversation_data.update(temp)
            except Exception as e:
                logger.error(f"Error processing JSON data: {e}")

    return conversation_data
