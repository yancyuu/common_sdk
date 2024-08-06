import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *
from common_sdk.system.sys_env import get_env
from common_sdk.logging.logger import logger
import backoff

class RetryException(Exception):
    """自定义异常类用于处理需要重试的情况"""
    pass


class BitableClient:
    
    def __init__(self, enable_token: bool = False, log_level: lark.LogLevel = lark.LogLevel.INFO):
        self.app_id = get_env("BITTABLE_APP_ID")
        self.app_secret = get_env("BITTABLE_APP_SECRET")
        self.enable_token = enable_token
        self.log_level = log_level
        self.client = self._create_client()

    def _create_client(self):
        builder = lark.Client.builder().app_id(self.app_id).app_secret(self.app_secret)
        if self.enable_token:
            builder.enable_set_token(True)
        builder.log_level(self.log_level)
        return builder.build()
    
    @backoff.on_exception(backoff.expo, RetryException, max_tries=3)
    async def acreate_app_table(self, app_token, name, view_name, fields_info):
        """
        创建应用表
        :param app_token: 应用令牌
        :param name: 表格名称
        :param folder_token: 文件夹令牌
        :param fields_info: 字段信息列表，每个元素是一个字典，包含 'field_name' 和 'type'
        :return: 创建结果
        """
        # 动态生成字段
        fields = []
        for field_info in fields_info:
            builder = AppTableCreateHeader.builder().field_name(field_info['field_name']).type(field_info['type'])
            if 'property' in field_info:
                builder.property(field_info['property'])
            if "ui_type" in field_info:
                builder.ui_type(field_info['ui_type'])
            fields.append(builder.build())

        request = CreateAppTableRequest.builder() \
            .app_token(app_token) \
            .request_body(CreateAppTableRequestBody.builder()
                .table(ReqTable.builder()
                    .name(name)
                    .default_view_name(view_name)
                    .fields(fields)
                    .build())
                .build()) \
            .build()

        response: CreateAppResponse = await self.client.bitable.v1.app_table.acreate(request)
        return self._process_response(response)

    @backoff.on_exception(backoff.expo, RetryException, max_tries=3)
    def list_app_table_view(self, app_token: str, table_id: str):
        request = ListAppTableViewRequest.builder().app_token(app_token).table_id(table_id).build()
        response = self.client.bitable.v1.app_table_view.list(request)
        return self._process_response(response)

    @backoff.on_exception(backoff.expo, RetryException, max_tries=3)
    async def alist_app_table_view(self, app_token: str, table_id: str):
        request = ListAppTableViewRequest.builder().app_token(app_token).table_id(table_id).build()
        response = await self.client.bitable.v1.app_table_view.alist(request)
        return self._process_response(response)

    @backoff.on_exception(backoff.expo, RetryException, max_tries=3)
    async def aget_app_table_fileds(self, app_token: str, table_id: str):
        request = ListAppTableFieldRequest.builder().app_token(app_token).table_id(table_id).build()
        response = await self.client.bitable.v1.app_table_field.alist(request)
        return self._process_response(response)
    
    @backoff.on_exception(backoff.expo, RetryException, max_tries=3)
    def get_app_table_fileds(self, app_token: str, table_id: str):
        request = ListAppTableFieldRequest.builder().app_token(app_token).table_id(table_id).build()
        response = self.client.bitable.v1.app_table_field.list(request)
        return self._process_response(response)

    @backoff.on_exception(backoff.expo, RetryException, max_tries=3)
    async def aget_app_table_fileds(self, app_token: str, table_id: str):
        request = ListAppTableFieldRequest.builder().app_token(app_token).table_id(table_id).build()
        response = await self.client.bitable.v1.app_table_field.alist(request)
        return self._process_response(response)

    @backoff.on_exception(backoff.expo, RetryException, max_tries=3)
    def get_app(self, app_token):
        request = GetAppRequest.builder().app_token(app_token).build()
        response: GetAppResponse = self.client.bitable.v1.app.get(request)
        return self._process_response(response)

    @backoff.on_exception(backoff.expo, RetryException, max_tries=3)
    async def aget_app(self, app_token):
        request = GetAppRequest.builder().app_token(app_token).build()
        response: GetAppResponse = await self.client.bitable.v1.app.aget(request)
        return self._process_response(response)
    
    @backoff.on_exception(backoff.expo, RetryException, max_tries=3)
    def get_app_table(self, app_token: str):
        request = ListAppTableRequest.builder().app_token(app_token).build()
        # 发起请求
        response: ListAppTableResponse = self.client.bitable.v1.app_table.list(request)
        return self._process_response(response)
    
    @backoff.on_exception(backoff.expo, RetryException, max_tries=3)
    async def aget_app_table(self, app_token: str):
        request = ListAppTableRequest.builder().app_token(app_token).build()
        # 发起请求
        response: ListAppTableResponse = await self.client.bitable.v1.app_table.alist(request)
        return self._process_response(response)

    @backoff.on_exception(backoff.expo, RetryException, max_tries=3)
    def get_table_view(self, app_token: str, table_id: str, view_id: str):
        request = GetAppTableViewRequest.builder()\
            .app_token(app_token) \
            .table_id(table_id) \
            .view_id(view_id) \
            .build()
        # 发起请求
        response: GetAppTableViewResponse = self.client.bitable.v1.app_table_view.get(request)
        return self._process_response(response)
    
    @backoff.on_exception(backoff.expo, RetryException, max_tries=3)
    def get_view_records(self, app_token: str, table_id: str, view_id: str):
        # 构造请求对象
        request = ListAppTableRecordRequest.builder() \
            .app_token(app_token)\
            .table_id(table_id) \
            .view_id(view_id) \
            .build()
        # 发起请求
        response: ListAppTableRecordResponse = self.client.bitable.v1.app_table_record.list(request)
        return self._process_response(response)
    
    @backoff.on_exception(backoff.expo, RetryException, max_tries=3)
    async def aget_view_records(self, app_token: str, table_id: str, view_id: str):
        # 构造请求对象
        request = ListAppTableRecordRequest.builder() \
            .app_token(app_token)\
            .table_id(table_id) \
            .view_id(view_id) \
            .build()
        # 发起请求
        response: ListAppTableRecordResponse = await self.client.bitable.v1.app_table_record.alist(request)
        return self._process_response(response)
    
    async def aget_one_records(self, app_token: str, table_id: str, record_id: str):
        # 构造请求对象
        request = GetAppTableRecordRequest.builder() \
            .app_token(app_token)\
            .table_id(table_id) \
            .record_id(record_id)\
            .build()
        # 发起请求
        response: GetAppTableRecordResponse = await self.client.bitable.v1.app_table_record.aget(request)
        return self._process_response(response)

    def list_all_records(self, app_token, table_id, view_id=None, filter=None):
        all_records = []
        page_token = None

        while True:
            # 构造请求对象
            request = ListAppTableRecordRequest.builder() \
                .app_token(app_token) \
                .table_id(table_id) \
                .view_id(view_id) \
                .page_size(500)
            if page_token:
                request = request.page_token(page_token)
            if filter:
                request = request.filter(filter)
            # 发起请求
            response: ListAppTableRecordResponse = self.client.bitable.v1.app_table_record.list(request.build())
            records = self._process_response(response)
            if records.items:
                all_records.extend(records.items)
            # 检查是否还有更多的记录
            has_more = records.has_more
            page_token = records.page_token if has_more else None

            if not has_more:
                break
        return all_records
    

    async def alist_all_records(self, app_token, table_id, view_id=None, filter=None):
        all_records = []
        page_token = None

        while True:
            # 构造请求对象
            request = ListAppTableRecordRequest.builder() \
                .app_token(app_token) \
                .table_id(table_id) \
                .page_size(500)
            if view_id:
                request = request.view_id(view_id)
            if page_token:
                request = request.page_token(page_token)
            if filter:
                request = request.filter(filter)
            # 发起请求
            response: ListAppTableRecordResponse = await self.client.bitable.v1.app_table_record.alist(request.build())
            records = self._process_response(response)
            if records.items:
                all_records.extend(records.items)
            # 检查是否还有更多的记录
            has_more = records.has_more
            page_token = records.page_token if has_more else None

            if not has_more:
                break
        return all_records
    
    @backoff.on_exception(backoff.expo, RetryException, max_tries=3)
    async def asearch_view_record(self, app_token, table_id, field_names=[] ,filter_conditions=None):
        # 根据条件构造搜索请求
        filter_info = FilterInfo.builder().conjunction("and").conditions(filter_conditions).build() if filter_conditions else None
        request = SearchAppTableRecordRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .request_body(SearchAppTableRecordRequestBody.builder()
                .field_names(field_names)\
                .filter(filter_info)\
                .build()) \
            .build()

        # 发起请求
        response: SearchAppTableRecordResponse = await self.client.bitable.v1.app_table_record.asearch(request)
        return self._process_response(response)

    @backoff.on_exception(backoff.expo, RetryException, max_tries=3)
    def update_view_record(self, app_token, table_id, record_id :str, fields :dict):
        # 构造请求对象
        request = BatchUpdateAppTableRecordRequest.builder() \
            .app_token(app_token)\
            .table_id(table_id) \
            .request_body(BatchUpdateAppTableRecordRequestBody.builder()
                .records([AppTableRecord.builder()
                    .fields(fields)
                    .record_id(record_id)
                    .build()
                    ])
                .build()) \
            .build()
        # 发起请求
        response: BatchUpdateAppTableRecordResponse = self.client.bitable.v1.app_table_record.batch_update(request)
        return self._process_response(response)
    
    @backoff.on_exception(backoff.expo, RetryException, max_tries=3)
    async def aupdate_view_record(self, app_token, table_id, record_id :str, fields :dict):
        # 构造请求对象
        request = BatchUpdateAppTableRecordRequest.builder() \
            .app_token(app_token)\
            .table_id(table_id) \
            .request_body(BatchUpdateAppTableRecordRequestBody.builder()
                .records([AppTableRecord.builder()
                    .fields(fields)
                    .record_id(record_id)
                    .build()
                    ])
                .build()) \
            .build()
        # 发起请求
        response: BatchUpdateAppTableRecordResponse = await self.client.bitable.v1.app_table_record.abatch_update(request)
        return self._process_response(response)

    @backoff.on_exception(backoff.expo, RetryException, max_tries=3)
    def delete_view_record(self, app_token, table_id, record_id):
        # 构造请求对象
        request = DeleteAppTableRecordRequest.builder() \
            .app_token(app_token)\
            .table_id(table_id) \
            .record_id(record_id) \
        .build()
        # 发起请求
        response: DeleteAppTableRecordResponse = self.client.bitable.v1.app_table_record.delete(request)
        return self._process_response(response)
    
    @backoff.on_exception(backoff.expo, RetryException, max_tries=3)
    def add_view_record(self, app_token, table_id, fields):
        # 构造请求对象
        request  = CreateAppTableRecordRequest.builder() \
            .app_token(app_token)\
            .table_id(table_id) \
            .request_body(AppTableRecord.builder()
                .fields(fields)
                .build()) \
            .build()
        # 发起请求
        response: CreateAppTableRecordResponse = self.client.bitable.v1.app_table_record.create(request)
        return self._process_response(response)

    @backoff.on_exception(backoff.expo, RetryException, max_tries=3)
    async def aadd_view_record(self, app_token, table_id, fields):
        # 构造请求对象
        request  = CreateAppTableRecordRequest.builder() \
            .app_token(app_token)\
            .table_id(table_id) \
            .request_body(AppTableRecord.builder()
                .fields(fields)
                .build()) \
            .build()
        # 发起请求
        response: CreateAppTableRecordResponse = await self.client.bitable.v1.app_table_record.acreate(request)
        return self._process_response(response)

    @backoff.on_exception(backoff.expo, RetryException, max_tries=3)
    def list_fields(self, app_token, table_id):
        # 构造请求对象
        request: ListAppTableFieldRequest = ListAppTableFieldRequest.builder() \
            .app_token(app_token)\
            .table_id(table_id) \
            .build()

        # 发起请求
        response: ListAppTableFieldResponse = self.client.bitable.v1.app_table_field.list(request)
        return self._process_response(response)

    def _process_response(self, response):
        if response.code == 1254607:
            raise RetryException(f"Request failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}")
        if not response.success():
            lark.logger.error(
                f"Request failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}")
            raise Exception(f"Request failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}")
        lark.logger.info(lark.JSON.marshal(response.data, indent=4))
        return response.data

    

