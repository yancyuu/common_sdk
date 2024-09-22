from pymilvus import MilvusClient, WeightedRanker, AnnSearchRequest, SearchFuture
from common_sdk.system.sys_env import get_env
from common_sdk.logging.logger import logger
from typing import List, Dict, Any


class MilvusRagClient(MilvusClient):
    """milvus客户端封装"""

    def __init__(self):
        super().__init__(
            uri=get_env('MILVUS_URI'),
            token=get_env('MILVUS_PASSWORD'),
             db_name=get_env('MILVUS_DB_NAME'))

    async def create_fields_collection(self, field_schema: List[Dict[str, Any]], index_params: List[Dict[str, Any]],
                                 schema_description: str, enable_dynamic_field: bool, collection_name: str):
        """
            说明：自定义 创建collection
            field_schema: [
                            {"field_name": "id", "datatype": DataType.INT64, "is_primary": True, "description": "主健", "auto_id": True},
                            {"field_name": "keywords_embedding", "datatype": DataType.FLOAT_VECTOR, "dim": 3072, "description": "关键词"}
                         ]
            index_params: [
                            {"field_name": "id", "index_type": "", "index_name": "id_index"},
                            {"field_name": "chunk_context_embedding", "metric_type": "COSINE", "index_type": "IVF_FLAT", "index_name": "chunk_context_embedding", "params": {"nlist": 128}},
                         ]
        """
        try:
            if not self.has_collection(collection_name):
                schema = self.create_schema(enable_dynamic_field=enable_dynamic_field,
                                            description=schema_description)
                for field in field_schema:
                    schema.add_field(**field)

                index_params_obj = self.prepare_index_params()
                for index in index_params:
                    index_params_obj.add_index(**index)

                self.create_collection(
                    collection_name=collection_name,
                    schema=schema,
                    index_params=index_params_obj,
                )
                return True
            logger.error(f"集合已存在: {collection_name}")
            return False
        except Exception as e:
            logger.error(f"创建集合失败: {e}")
            raise e

    def collection_insert(self, collection_name: str, data: List[Dict]):
        """插入数据"""
        try:
            self.insert(collection_name=collection_name, data=data)
            return True
        except Exception as e:
            logger.error(f"插入失败: {e}")
            raise e

    def delete_collection_data(self,collection_name: str, ids: List[Any]):
        try:
            self.delete(
                collection_name=collection_name,
                ids=ids,
            )
            return True
        except Exception as e:
            logger.error(f"删除失败: {e}")
            raise e

    def hybrid_search(self,collection_name:str, reqs:List[AnnSearchRequest], rerank:WeightedRanker, limit:int, _async:bool=False,**kwargs):
        try:
            con = self._get_connection()
            resp = con.hybrid_search(collection_name=collection_name,reqs=reqs, rerank=rerank, limit=limit, _async=_async)
            return SearchFuture(resp) if _async else resp
        except Exception as e:
            logger.error(f"混合搜索失败: {e}")
            raise e