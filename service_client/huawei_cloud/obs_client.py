from obs import ObsClient, PutObjectHeader, GetObjectHeader
from urllib.parse import quote
import os
import traceback
import logging

# 获取环境变量中的 AK/SK 和 Endpoint
ak = "18R2DRJGPU82MIDXTSAF"
sk = "w0DYTTuJCRdLWDI9qCaGZGPIblKpOdvHvooY8DKd"
server = "https://obs.ap-southeast-1.myhuaweicloud.com"

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ObsClientWrapper:
    def __init__(self, bucket_name):
        self.client = ObsClient(
            access_key_id=ak,
            secret_access_key=sk,
            server=server
        )
        self.bucket_name = bucket_name

    def upload_file(self, object_name, file_path):
        try:
            headers = PutObjectHeader()
            # 设置MIME类型，可以根据实际情况修改
            headers.contentType = 'application/octet-stream'
            # 自定义元数据
            metadata = {'name': object_name}

            # 上传文件
            resp = self.client.putFile(self.bucket_name, object_name, file_path, metadata, headers)

            # 检查上传结果
            if resp.status < 300:
                logger.info(f"File '{object_name}' uploaded successfully")
                # 编码object_name以确保URL的有效性
                encoded_object_name = quote(object_name, safe='')
                # 构造并返回文件URL
                file_url = f"http://{self.bucket_name}.{server}/{encoded_object_name}"
                return file_url
            else:
                logger.error(f"Failed to upload file '{object_name}': {resp.errorMessage}")
                return None
        except Exception as e:
            logger.error(f"Exception when uploading file '{object_name}': {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def download_file(self, object_name, local_file_path):
        try:
            headers = GetObjectHeader()

            # 文件下载
            resp = self.client.getObject(self.bucket_name, object_name, local_file_path, headers=headers)

            # 检查下载结果
            if resp.status < 300:
                logger.info(f"File '{object_name}' downloaded successfully to '{local_file_path}'")
                return resp.body
            else:
                logger.error(f"Failed to download file '{object_name}': {resp.errorMessage}")
                return None
        except Exception as e:
            logger.error(f"Exception when downloading file '{object_name}': {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def list_objects(self, prefix=None):
        try:
            resp = self.client.listObjects(self.bucket_name, prefix=prefix)
            if resp.status < 300:
                files = [content.key for content in resp.body.contents]
                for file in files:
                    logger.info(f"Found file: {file}")
                return files
            else:
                logger.error(f"Failed to list files: {resp.errorMessage}")
                return []
        except Exception as e:
            logger.error(f"Exception when listing files: {str(e)}")
            logger.error(traceback.format_exc())
            return []

    def delete_object(self, object_name):
        try:
            resp = self.client.deleteObject(self.bucket_name, object_name)
            if resp.status < 300:
                logger.info(f"File '{object_name}' deleted successfully")
                return True
            else:
                logger.error(f"Failed to delete file '{object_name}': {resp.errorMessage}")
                return False
        except Exception as e:
            logger.error(f"Exception when deleting file '{object_name}': {str(e)}")
            logger.error(traceback.format_exc())
            return False


if __name__ == "__main__":
    # 使用示例
    obs_client = ObsClientWrapper("examplebucket")
    folder_res = obs_client.create_folder("test_folder")
    print(f"create_folder res: {folder_res}")

    file_path = '/path/to/localfile.txt'
    upload_res = obs_client.upload_file('test_folder/example.txt', file_path)
    print(f"upload_file res: {upload_res}")

    download_res = obs_client.download_file('test_folder/example.txt', '/path/to/downloadedfile.txt')
    print(f"download_file res: {download_res}")

    list_res = obs_client.list_files('test_folder/')
    print(f"list_files res: {list_res}")

    delete_res = obs_client.delete_file('test_folder/example.txt')
    print(f"delete_file res: {delete_res}")
