import asyncio
from common_sdk.logging.logger import logger
from obs import ObsClient, PutObjectHeader, GetObjectHeader
from urllib.parse import quote
from io import BytesIO


class AsyncObsClient:
    """
    AsyncObsClient 是异步版的 OBS 客户端，用于与对象存储服务交互。
    """

    def __init__(self, ak: str, sk: str, server: str, bucket_name: str):
        """
        初始化 AsyncObsClient 实例。

        Args:
            ak (str): 访问密钥（Access Key）。
            sk (str): 私有密钥（Secret Key）。
            server (str): OBS 服务地址。
            bucket_name (str): 存储桶名称。
        """
        self.server = f"https://{server}"
        self.client = ObsClient(
            access_key_id=ak,
            secret_access_key=sk,
            server=self.server,
        )
        self.host = server
        self.bucket_name = bucket_name
        self.read_url = f"https://{bucket_name}.{server}"

    async def __run_in_thread(self, func, *args, **kwargs):
        """
        通用线程池执行器，用于运行同步阻塞操作。

        Args:
            func: 同步函数
            *args: 函数的位置参数
            **kwargs: 函数的关键字参数

        Returns:
            Any: 同步函数的返回值
        """
        return await asyncio.to_thread(func, *args, **kwargs)

    def __build_file_url(self, object_name: str) -> str:
        """
        构建 OBS 文件访问的 URL。

        Args:
            object_name (str): OBS 对象名称。

        Returns:
            str: 文件的完整访问 URL。
        """
        encoded_object_name = quote(object_name, safe="")
        return f"{self.read_url}/{encoded_object_name}"

    async def _upload(self, upload_func, *args):
        """
        通用上传逻辑，供文件和数据流上传复用。

        Args:
            upload_func: 执行上传的同步函数（如 putFile 或 putContent）。
            *args: 上传函数的参数。

        Returns:
            str: 文件访问 URL（成功时）。
            None: 失败时返回 None。
        """
        try:
            resp = await self.__run_in_thread(upload_func, *args)
            logger.info(f"resp----> '{resp}")

            if resp.status < 300:
                object_name = args[1]  # 第二个参数是 object_name
                logger.info(f"File '{object_name}' uploaded successfully.")
                return self.__build_file_url(object_name)
            else:
                logger.error(f"Failed to upload file '{args[1]}': {resp.errorMessage}")
                return None
        except Exception as e:
            logger.error(f"Exception occurred during upload: {e}")
            return None

    async def upload_file(self, object_name: str, file_path: str) -> str:
        """
        异步上传文件到 OBS。

        Args:
            object_name (str): 文件在 OBS 中的对象名称。
            file_path (str): 本地文件路径。

        Returns:
            str: 文件的访问 URL（成功时）。
            None: 上传失败时返回 None。
        """
        headers = PutObjectHeader(acl='public-read', contentType="application/octet-stream")
        metadata = {"name": object_name}
        return await self._upload(
            self.client.putFile, self.bucket_name, object_name, file_path, metadata, headers
        )

    async def upload_file_from_stream(self, object_name: str, data_stream: BytesIO) -> str:
        """
        异步上传数据流到 OBS。

        Args:
            object_name (str): 文件在 OBS 中的对象名称。
            data_stream (BytesIO): 包含文件数据的内存数据流。

        Returns:
            str: 文件的访问 URL（成功时）。
            None: 上传失败时返回 None。
        """
        headers = PutObjectHeader(acl='public-read', contentType="application/octet-stream")
        metadata = {"name": object_name}
        logger.info(f"self.bucket_name {self.bucket_name} metadata  {metadata}")

        return await self._upload(
            self.client.putContent, self.bucket_name, object_name, data_stream.getvalue(), metadata, headers
        )

    async def _download(self, object_name: str, local_file_path: str) -> BytesIO:
        """
        通用文件下载逻辑。

        Args:
            object_name (str): 文件在 OBS 中的对象名称。
            local_file_path (str): 保存到本地的路径。

        Returns:
            BytesIO: 文件内容（成功时）。
            None: 下载失败时返回 None。
        """
        headers = GetObjectHeader()
        try:
            resp = await self.__run_in_thread(
                self.client.getObject, self.bucket_name, object_name, local_file_path, headers=headers
            )
            if resp.status < 300:
                logger.info(f"File '{object_name}' downloaded successfully to '{local_file_path}'.")
                return resp.body
            else:
                logger.error(f"Failed to download file '{object_name}': {resp.errorMessage}")
                return None
        except Exception as e:
            logger.exception(f"Exception occurred during file download: {e}")
            return None

    async def download_file(self, object_name: str, local_file_path: str) -> BytesIO:
        """
        异步下载文件到本地。

        Args:
            object_name (str): 文件在 OBS 中的对象名称。
            local_file_path (str): 保存到本地的路径。

        Returns:
            BytesIO: 文件内容（成功时）。
            None: 下载失败时返回 None。
        """
        return await self._download(object_name, local_file_path)

    async def list_objects(self, prefix: str = None) -> list:
        """
        列举存储桶中的对象。

        Args:
            prefix (str, optional): 对象名称的前缀，用于筛选。默认为 None。

        Returns:
            list: 对象名称列表。
        """
        try:
            resp = await self.__run_in_thread(self.client.listObjects, self.bucket_name, prefix=prefix)
            if resp.status < 300:
                files = [content.key for content in resp.body.contents]
                logger.info(f"Found files: {files}")
                return files
            else:
                logger.error(f"Failed to list files: {resp.errorMessage}")
                return []
        except Exception as e:
            logger.exception(f"Exception occurred during list objects: {e}")
            return []

    async def delete_object(self, object_name: str) -> bool:
        """
        删除存储桶中的对象。

        Args:
            object_name (str): 要删除的对象名称。

        Returns:
            bool: 删除成功返回 True，否则返回 False。
        """
        try:
            resp = await self.__run_in_thread(self.client.deleteObject, self.bucket_name, object_name)
            if resp.status < 300:
                logger.info(f"File '{object_name}' deleted successfully.")
                return True
            else:
                logger.error(f"Failed to delete file '{object_name}': {resp.errorMessage}")
                return False
        except Exception as e:
            logger.exception(f"Exception occurred during file deletion: {e}")
            return False
