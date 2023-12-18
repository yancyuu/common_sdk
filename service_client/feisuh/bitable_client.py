import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *
from common_sdk.system.sys_env import get_env

class BitableClient:
    def __init__(self, enable_token: bool = False, log_level: lark.LogLevel = lark.LogLevel.DEBUG):
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

    def list_app_table_view(self, app_token: str, table_id: str):
        request = ListAppTableViewRequest.builder().app_token(app_token).table_id(table_id).build()
        response = self.client.bitable.v1.app_table_view.list(request)
        return self._process_response(response)

    def get_app(self, app_token):
        request = GetAppRequest.builder().app_token(app_token).build()
        response: GetAppResponse = self.client.bitable.v1.app.get(request)
        return self._process_response(response)
    
    def get_app_table(self, app_token: str):
        request = ListAppTableRequest.builder().app_token(app_token).build()
        # 发起请求
        response: ListAppTableResponse = self.client.bitable.v1.app_table.list(request)
        return self._process_response(response)

    def get_table_view(self, app_token: str, table_id: str, view_id: str):
        request = GetAppTableViewRequest.builder()\
            .app_token(app_token) \
            .table_id(table_id) \
            .view_id(view_id) \
            .build()
        # 发起请求
        response: GetAppTableViewResponse = self.client.bitable.v1.app_table_view.get(request)
        return self._process_response(response)
    
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
                request = request.filter(page_token)
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


    def _process_response(self, response):
        if not response.success():
            lark.logger.error(
                f"Request failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}")
            return None
        lark.logger.info(lark.JSON.marshal(response.data, indent=4))
        return response.data

# 使用实例
if __name__ == "__main__":
    # 使用你的APP_ID和APP_SECRET初始化客户端
    app_id = "cli_a5c23e508a7b500e"
    app_secret = "AsQRru43gx9x6qtPpTInihERAfvqeQZG"
    lark_client = BitableClient(app_id, app_secret, enable_token=True)

    # 调用 list_app_table_view
    app_token = "CQJTb7GtiapxgpspLGpchQmPnyf"
    table_id = "tbl04PY8IJhZquiB"

    app_table_data = lark_client.list_app_table_view(app_token, table_id)
    if app_table_data:
        print(f"获取表格视图成功 {app_table_data}")
    
    view_id = "vewOuzouXv"
    get_table_view = lark_client.get_table_view(app_token, table_id, view_id)
    print(f"获取视图成功 {get_table_view}")

    get_view_records = lark_client.get_view_records(app_token, table_id, view_id)
    print(f"获取视图内容成功 {get_table_view}")

    res = lark_client.update_view_records(app_token, table_id, record_id="recjy6lzNb", fields={"来源": "127.0.0.1", "日期": 1674206443000})
    print(f"更新视图内容成功 {res}")

    

