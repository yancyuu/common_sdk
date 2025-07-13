from common_sdk.logging.logger import logger
import asyncio
from urllib.parse import quote
import oss2
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, BinaryIO, Union


class OSSClient:
    def __init__(self,
                 access_key_id: str = None,
                 access_key_secret: str = None,
                 endpoint: str = None,
                 bucket_name: str = None):
        """
        初始化OSS客户端

        Args:
            access_key_id: 访问密钥ID，如果为None则从环境变量获取
            access_key_secret: 访问密钥，如果为None则从环境变量获取
            endpoint: OSS端点
            bucket_name: 存储桶名称
        """
        self.access_key_id = access_key_id or os.getenv('OSS_ACCESS_KEY_ID')
        self.access_key_secret = access_key_secret or os.getenv('OSS_ACCESS_KEY_SECRET')
        self.endpoint = endpoint or os.getenv('OSS_ENDPOINT', 'https://oss-cn-hangzhou.aliyuncs.com')
        self.bucket_name = bucket_name or os.getenv('OSS_BUCKET_NAME')

        if not all([self.access_key_id, self.access_key_secret, self.endpoint, self.bucket_name]):
            raise ValueError("Missing required OSS configuration parameters")

        # 创建认证对象
        self.auth = oss2.Auth(self.access_key_id, self.access_key_secret)

        # 创建Bucket对象
        self.bucket = oss2.Bucket(self.auth, self.endpoint, self.bucket_name)

        # 创建线程池用于异步执行
        self.executor = ThreadPoolExecutor(max_workers=4)

    async def upload_file(self, object_name: str, file_data: Union[bytes, BinaryIO, str]) -> bool:
        """
        异步上传文件

        Args:
            object_name: 对象名称（文件路径）
            file_data: 文件数据，可以是bytes、文件对象或文件路径

        Returns:
            上传成功后的文件URL
        """
        try:
            def _upload():
                result = self.bucket.put_object(object_name, file_data)
                return result

            # 在线程池中执行同步操作
            result = await asyncio.get_event_loop().run_in_executor(
                self.executor, _upload
            )
            if result.status == 200:  # 确保上传成功
                return True
            raise Exception(f"Failed to upload file to OSS. Status code: {result.status}")

        except Exception as e:
            logger.error(f"文件上传失败: {str(e)}")
            raise

    async def download_file(self, object_name: str) -> bytes:
        """
        异步下载文件

        Args:
            object_name: 对象名称（文件路径）

        Returns:
            文件内容（bytes）
        """
        try:
            def _download():
                result = self.bucket.get_object(object_name)
                return result.read()

            content = await asyncio.get_event_loop().run_in_executor(
                self.executor, _download
            )

            logger.info(f"文件下载成功: {object_name}")
            return content

        except Exception as e:
            logger.error(f"文件下载失败: {str(e)}")
            raise

    async def delete_file(self, object_name: str) -> bool:
        """
        异步删除文件

        Args:
            object_name: 对象名称（文件路径）

        Returns:
            删除是否成功
        """
        try:
            def _delete():
                result = self.bucket.delete_object(object_name)
                return result

            result = await asyncio.get_event_loop().run_in_executor(
                self.executor, _delete
            )

            logger.info(f"文件删除成功: {object_name}, 状态码: {result.status}")
            return True

        except Exception as e:
            logger.error(f"文件删除失败: {str(e)}")
            return False

    async def create_folder(self, folder_name: str) -> bool:
        """
        异步创建文件夹（如果不存在）

        Args:
            folder_name: 文件夹名称，必须以 '/' 结尾

        Returns:
            创建是否成功
        """
        try:
            # 确保文件夹名以 '/' 结尾
            if not folder_name.endswith('/'):
                folder_name += '/'

            def _check_and_create():
                # 检查文件夹是否存在
                try:
                    self.bucket.get_object(folder_name)
                    return True  # 文件夹已存在
                except oss2.exceptions.NoSuchKey:
                    # 文件夹不存在，创建它
                    result = self.bucket.put_object(folder_name, '')
                    return result.status == 200

            success = await asyncio.get_event_loop().run_in_executor(
                self.executor, _check_and_create
            )

            if success:
                logger.info(f"文件夹创建/确认成功: {folder_name}")
            return success

        except Exception as e:
            logger.error(f"文件夹创建失败: {str(e)}")
            return False

    async def list_objects(self, prefix: str = '', max_keys: int = 100) -> list:
        """
        异步列出对象

        Args:
            prefix: 对象名前缀
            max_keys: 返回的最大对象数量

        Returns:
            对象列表
        """
        try:
            def _list():
                objects = []
                for obj in oss2.ObjectIterator(self.bucket, prefix=prefix, max_keys=max_keys):
                    objects.append({
                        'key': obj.key,
                        'size': obj.size,
                        'last_modified': obj.last_modified,
                        'etag': obj.etag
                    })
                return objects

            objects = await asyncio.get_event_loop().run_in_executor(
                self.executor, _list
            )

            logger.info(f"对象列表获取成功，前缀: {prefix}, 数量: {len(objects)}")
            return objects

        except Exception as e:
            logger.error(f"对象列表获取失败: {str(e)}")
            return []

    async def object_exists(self, object_name: str) -> bool:
        """
        异步检查对象是否存在

        Args:
            object_name: 对象名称

        Returns:
            对象是否存在
        """
        try:
            def _exists():
                try:
                    self.bucket.head_object(object_name)
                    return True
                except oss2.exceptions.NoSuchKey:
                    return False

            exists = await asyncio.get_event_loop().run_in_executor(
                self.executor, _exists
            )

            return exists

        except Exception as e:
            logger.error(f"检查对象存在性失败: {str(e)}")
            return False

    async def get_file_url(self, object_name: str, expires: int = 3600) -> str:
        """
        异步生成预签名URL

        Args:
            object_name: 对象名称
            expires: URL过期时间（秒）

        Returns:
            预签名URL
        """
        try:
            def _generate_url():
                return self.bucket.sign_url('GET', object_name, expires)

            url = await asyncio.get_event_loop().run_in_executor(
                self.executor, _generate_url
            )

            logger.info(f"预签名URL生成成功: {object_name}")
            return url

        except Exception as e:
            logger.error(f"预签名URL生成失败: {str(e)}")
            raise

    async def close(self):
        """关闭线程池"""
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=True)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


# 创建全局实例
def get_oss_client() -> OSSClient:
    """获取OSS客户端实例"""
    return OSSClient()


