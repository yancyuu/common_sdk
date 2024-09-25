from common_sdk.system.sys_env import get_env
from common_sdk.logging.logger import logger
from urllib.parse import quote
import oss2

class OSSClient:
    def __init__(self, bucket_name):
        self.access_key_id = get_env("OSS_ACCESS_KEY_ID")
        self.access_key_secret = get_env("OSS_ACCESS_KEY_SECRET")
        self.endpoint = get_env("OSS_ENDPOINT")
        self.bucket_name = bucket_name
        self.bucket = oss2.Bucket(oss2.Auth(self.access_key_id, self.access_key_secret), "http://"+self.endpoint, self.bucket_name)

    def create_folder(self, folder_name):
        try:
            # 确保文件夹名称以'/'结尾
            if not folder_name.endswith('/'):
                folder_name += '/'
            # 创建一个以'/'结尾的空对象来模拟文件夹
            self.bucket.put_object(folder_name, '')
            logger.info(f"Folder '{folder_name}' created")
            return folder_name
        except (oss2.exceptions.OssError, oss2.exceptions.ServerError) as e:
            return

    def upload_file(self, object_name, file):
        try:
            self.bucket.put_object(object_name, file)
            logger.info(f"File uploaded as {object_name}")
            # 编码object_name以确保URL的有效性
            encoded_object_name = quote(object_name, safe='')
            # 构造并返回文件URL
            file_url = f"http://{self.bucket_name}.{self.endpoint}/{encoded_object_name}"
            return file_url
        except (oss2.exceptions.OssError, oss2.exceptions.ServerError) as e:
            # 日志记录错误信息
            logger.error(f"Failed to upload file: {e}")
            return None
        
    def download_file(self, object_name, local_file_path):
        try:
            res = self.bucket.get_object_to_file(object_name, local_file_path)
            logger.info(f"File {object_name} downloaded to {local_file_path} res {res}")
            return res.resp
        except (oss2.exceptions.OssError, oss2.exceptions.ServerError) as e:
            return

    def list_files(self, prefix=None):
        res = []
        for object_info in oss2.ObjectIterator(self.bucket, prefix=prefix):
            res.append(object_info)
            logger.info(f"{object_info.last_modified} {object_info.key} res {res}")
        return res

    def delete_file(self, object_name):
        try:
            res = self.bucket.delete_object(object_name)
            logger.info(f"File {object_name} deleted")
            return res.resp
        except (oss2.exceptions.OssError, oss2.exceptions.ServerError) as e:
            return

    def get_object(self, object_name):
        try:
            res = self.bucket.get_object(object_name)
            logger.info(f"Read file success {object_name}")
            return res.resp
        except (oss2.exceptions.OssError, oss2.exceptions.ServerError) as e:
            return
        
    
if __name__ == "__main__":
    # 使用示例
    oss_client = OSSClient("zsxq-spider-files")
    res = oss_client.create_folder("0305")
    print(f"create_folder res {res}")
    file_path = r'/Users/skg/semantra/pdf/files/腾讯：食品饮料品类大剧内容营销指南报告2023年版.pdf'
    with open(file_path, "rb") as file:
        res = oss_client.upload_file(r'0305/example.pdf', file)
        print(f"upload_file res {res}")
    res = oss_client.list_files()
    print(f"list_files res {res}")
