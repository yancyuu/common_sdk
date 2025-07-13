from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.request import CommonRequest
from aliyunsdkcore.acs_exception.exceptions import ServerException, ClientException
from common_sdk.logging.logger import logger
import json
import time
import base64
import hmac
import hashlib
from datetime import datetime, timedelta

# 尝试导入STS专用请求类，如果不可用则使用CommonRequest
try:
    from aliyunsdksts.request.v20150401 import AssumeRoleRequest
    HAS_STS_SDK = True
except ImportError:
    logger.warning("aliyunsdksts包未安装，将使用CommonRequest作为备用方案")
    HAS_STS_SDK = False


class PolicyConditions:
    """OSS策略条件常量"""
    COND_CONTENT_LENGTH_RANGE = "content-length-range"
    COND_KEY = "key"


class MatchMode:
    """OSS策略匹配模式常量"""
    STARTS_WITH = "starts-with"
    EXACT = "eq"


class AliyunTokenGenerator:
    def __init__(self, ak_id, ak_secret):
        self.ak_id = ak_id
        self.ak_secret = ak_secret

    def _add_policy_condition(self, conditions_list, condition_type, *args):
        """
        添加策略条件
        模仿Java的PolicyConditions.addConditionItem方法
        
        Args:
            conditions_list: 条件列表
            condition_type: 条件类型
            *args: 条件参数
        """
        if condition_type == PolicyConditions.COND_CONTENT_LENGTH_RANGE:
            if len(args) != 2:
                raise ValueError("COND_CONTENT_LENGTH_RANGE 需要 min 和 max 两个参数")
            min_size, max_size = args
            conditions_list.append([condition_type, min_size, max_size])
        elif condition_type == PolicyConditions.COND_KEY:
            if len(args) != 2:
                raise ValueError("COND_KEY 需要 match_mode 和 value 两个参数")
            match_mode, value = args
            conditions_list.append([match_mode, f"${condition_type}", value])
        else:
            # 通用条件添加
            conditions_list.append([condition_type] + list(args))

    def get_oss_sts(self, region, role_arn):
        """获取OSS STS临时访问凭证"""
        try:
            # 创建客户端
            client = AcsClient(self.ak_id, self.ak_secret, region)
            
            if HAS_STS_SDK:
                # 使用专用的STS SDK
                request = AssumeRoleRequest.AssumeRoleRequest()
                request.set_accept_format('json')
                request.set_RoleArn(role_arn)
                request.set_RoleSessionName('oss-assume-role-session')
                request.set_DurationSeconds(3600)  # 1小时有效期
            else:
                # 使用CommonRequest作为备用方案
                request = CommonRequest()
                request.set_accept_format('json')
                request.set_domain('sts.aliyuncs.com')
                request.set_method('POST')
                request.set_protocol_type('https')
                request.set_version('2015-04-01')
                request.set_action_name('AssumeRole')
                request.add_query_param('RoleArn', role_arn)
                request.add_query_param('RoleSessionName', 'oss-assume-role-session')
                request.add_query_param('DurationSeconds', '3600')
            
            # 执行请求
            response = client.do_action_with_exception(request)
            
            # 确保response是字符串
            if response is None:
                raise RuntimeError("API响应为空")
            
            if isinstance(response, bytes):
                response = response.decode('utf-8')
            
            # 解析响应
            response_data = json.loads(response)
            logger.info(f"STS response: {response_data}")
            
            # 提取凭证信息
            credentials = response_data.get("Credentials")
            if not credentials:
                raise RuntimeError(f"AssumeRole missing Credentials: {response_data}")
            
            logger.info("STS token获取成功")
            return {
                "AccessKeyId": credentials["AccessKeyId"],
                "AccessKeySecret": credentials["AccessKeySecret"],
                "SecurityToken": credentials["SecurityToken"],
                "Expiration": credentials["Expiration"]
            }
            
        except (ServerException, ClientException) as e:
            logger.error(f"阿里云STS服务异常: {e}")
            raise RuntimeError(f"OSS STS服务异常: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"STS响应解析失败: {e}")
            raise RuntimeError(f"STS响应解析失败: {e}")
        except Exception as e:
            logger.error(f"获取STS token失败: {e}")
            raise RuntimeError(f"获取STS token失败: {e}")

    def get_oss_sts_with_policy(self, region, role_arn, policy=None, bucket=None, upload_path=None, host=None, expire_time=360, max_size=5242880000, return_post_policy=False):
        """
        获取带自定义策略的OSS STS临时访问凭证
        支持同时生成PostObject签名策略
        
        Args:
            return_post_policy: 是否同时返回PostObject签名策略
            bucket: OSS存储桶名称（当return_post_policy=True时必需）
            upload_path: 上传路径前缀（当return_post_policy=True时必需）
            host: OSS域名（当return_post_policy=True时必需）
            expire_time: PostObject过期时间（秒），默认360秒
            max_size: 最大文件大小，默认5GB
        """
        try:
            # 创建客户端
            client = AcsClient(self.ak_id, self.ak_secret, region)
            
            if HAS_STS_SDK:
                # 使用专用的STS SDK
                request = AssumeRoleRequest.AssumeRoleRequest()
                request.set_accept_format('json')
                request.set_RoleArn(role_arn)
                request.set_RoleSessionName('oss-assume-role-session-with-policy')
                request.set_DurationSeconds(3600)

                # 如果提供了策略，设置策略
                if policy:
                    if isinstance(policy, dict):
                        policy = json.dumps(policy)
                    request.set_Policy(policy)
            else:
                # 使用CommonRequest作为备用方案
                request = CommonRequest()
                request.set_accept_format('json')
                request.set_domain('sts.aliyuncs.com')
                request.set_method('POST')
                request.set_protocol_type('https')
                request.set_version('2015-04-01')
                request.set_action_name('AssumeRole')
                request.add_query_param('RoleArn', role_arn)
                request.add_query_param('RoleSessionName', 'oss-assume-role-session-with-policy')
                request.add_query_param('DurationSeconds', '3600')

                # 如果提供了策略，设置策略
                if policy:
                    if isinstance(policy, dict):
                        policy = json.dumps(policy)
                    request.add_query_param('Policy', policy)

            # 执行请求
            response = client.do_action_with_exception(request)

            # 确保response是字符串
            if response is None:
                raise RuntimeError("API响应为空")

            if isinstance(response, bytes):
                response = response.decode('utf-8')

            # 解析响应
            response_data = json.loads(response)
            logger.info(f"STS with policy response: {response_data}")

            # 提取凭证信息
            credentials = response_data.get("Credentials")
            if not credentials:
                raise RuntimeError(f"AssumeRole missing Credentials: {response_data}")

            sts_result = {
                "AccessKeyId": credentials["AccessKeyId"],
                "AccessKeySecret": credentials["AccessKeySecret"],
                "SecurityToken": credentials["SecurityToken"],
                "Expiration": credentials["Expiration"]
            }

            # 如果需要生成PostObject签名策略
            if return_post_policy:
                if not all([bucket, upload_path, host]):
                    raise ValueError("当return_post_policy=True时，bucket、upload_path、host参数都必须提供")
                
                # 生成PostObject签名策略
                post_policy = self.generate_oss_post_policy(bucket, upload_path, host, expire_time, max_size)
                
                # 合并结果
                result = {
                    "sts_credentials": sts_result,
                    "post_policy": post_policy
                }
                logger.info("STS凭证和PostObject签名策略获取成功")
                return result
            else:
                logger.info("带策略的STS token获取成功")
                return sts_result

        except (ServerException, ClientException) as e:
            logger.error(f"阿里云STS服务异常: {e}")
            raise RuntimeError(f"OSS STS服务异常: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"STS响应解析失败: {e}")
            raise RuntimeError(f"STS响应解析失败: {e}")
        except Exception as e:
            logger.error(f"获取STS token失败: {e}")
            raise RuntimeError(f"获取STS token失败: {e}")

    def validate_credentials(self, access_key_id, access_key_secret, security_token=None):
        """验证凭证是否有效"""
        try:
            # 使用临时凭证创建客户端
            if security_token:
                client = AcsClient(access_key_id, access_key_secret, 'cn-hangzhou', credential=security_token)
            else:
                client = AcsClient(access_key_id, access_key_secret, 'cn-hangzhou')

            # 创建GetCallerIdentity请求来验证凭证
            request = CommonRequest()
            request.set_accept_format('json')
            request.set_domain('sts.aliyuncs.com')
            request.set_method('POST')
            request.set_protocol_type('https')
            request.set_version('2015-04-01')
            request.set_action_name('GetCallerIdentity')

            # 执行请求
            response = client.do_action_with_exception(request)

            # 确保response是字符串
            if response is None:
                raise RuntimeError("API响应为空")

            if isinstance(response, bytes):
                response = response.decode('utf-8')

            response_data = json.loads(response)
            
            logger.info(f"凭证验证结果: {response_data}")
            return response_data
            
        except (ServerException, ClientException) as e:
            logger.error(f"凭证验证失败: {e}")
            return None
        except Exception as e:
            logger.error(f"凭证验证异常: {e}")
            return None

    def generate_oss_post_policy(self, bucket, upload_path, host, expire_time=360, max_size=5242880000):
        """
        生成OSS PostObject签名策略
        模仿Java代码实现OSS直传签名
        
        Args:
            bucket: OSS存储桶名称
            upload_path: 上传路径前缀
            host: OSS域名
            expire_time: 过期时间（秒），默认360秒
            max_size: 最大文件大小，默认5GB
        
        Returns:
            dict: 包含签名信息的字典
        """
        try:
            # 设置过期时间
            expire_end_time = int(time.time()) + expire_time
            expiration = datetime.utcfromtimestamp(expire_end_time).strftime('%Y-%m-%dT%H:%M:%S.000Z')
            
            # 创建策略条件 - 使用类似Java的方式
            policy_conditions = []
            
            # 添加文件大小限制条件 - 模仿Java: policyConds.addConditionItem(PolicyConditions.COND_CONTENT_LENGTH_RANGE, min: 0, max: 5242880000L)
            self._add_policy_condition(policy_conditions, PolicyConditions.COND_CONTENT_LENGTH_RANGE, 0, max_size)
            
            # 添加上传路径前缀限制条件 - 模仿Java: policyConds.addConditionItem(MatchMode.startWith, PolicyConditions.COND_KEY, uploadPath)
            self._add_policy_condition(policy_conditions, PolicyConditions.COND_KEY, MatchMode.STARTS_WITH, upload_path)
            
            # 构建策略文档
            policy_document = {
                "expiration": expiration,
                "conditions": policy_conditions
            }
            
            # 将策略转换为JSON字符串
            post_policy = json.dumps(policy_document, separators=(',', ':'))
            logger.info(f"Generated policy: {post_policy}")
            
            # 转换为base64编码
            encoded_policy = base64.b64encode(post_policy.encode('utf-8')).decode('utf-8')
            
            # 计算签名
            post_signature = base64.b64encode(
                hmac.new(
                    self.ak_secret.encode('utf-8'),
                    encoded_policy.encode('utf-8'),
                    hashlib.sha1
                ).digest()
            ).decode('utf-8')
            
            # 封装返回参数
            response = {
                "accessid": self.ak_id,
                "policy": encoded_policy,
                "signature": post_signature,
                "bucket": bucket,
                "dir": upload_path,
                "host": host,
                "expire": str(expire_end_time)
            }
            
            logger.info("OSS PostObject签名策略生成成功")
            return response
            
        except Exception as e:
            logger.error(f"生成OSS PostObject签名策略失败: {e}")
            raise RuntimeError(f"生成OSS PostObject签名策略失败: {e}")

