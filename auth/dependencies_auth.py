from common_sdk.logging.logger import logger
from common_sdk.auth.jwt_auth import jwt_auth
from common_sdk.util import id_generator, context
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, HTTPBasic, HTTPBasicCredentials

# 初始化 HTTP Basic Authentication
security = HTTPBasic()
# 定义允许的用户名和密码
USERNAME = "admin"
PASSWORD = "password123"


# 验证函数
def validate_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = credentials.username == USERNAME
    correct_password = credentials.password == PASSWORD
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )


bearer_security = HTTPBearer()


def validate_token(credentials: HTTPAuthorizationCredentials = Depends(bearer_security)):
    """
    验证 Bearer Token 的合法性

    :param credentials: 提取的 Bearer Token
    :return: 解码后的 Token Payload
    """
    token = credentials.credentials
    logger.info(f"token --> {token}")
    if not jwt_auth.check_token(token):
        logger.warning("Token verification failed")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or expired token",
        )
    payload = jwt_auth.decode_token(token)
    if not payload or not isinstance(payload, dict):
        logger.warning("Decoded token payload is invalid")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token payload",
        )

    # 将解析后的数据存入上下文（仅在中间件中使用）
    context.set("payload_data", payload.get("data", {}))
    user_id = payload.get("data", {}).get("id")
    if not user_id:
        logger.warning("Token payload missing user ID")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing user ID in token",
        )
    context.set("id", user_id)

    return payload  # 返回解码后的 Token 数据
