import os
from datetime import datetime, timedelta, timezone
import bcrypt
import jwt
from fastapi import HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

# Настройки JWT
JWT_SECRET = os.getenv("JWT_SECRET", "super_secret_key_change_me_in_production")
JWT_ALGORITHM = "HS256"
TOKEN_EXPIRE_HOURS = 24

security_bearer = HTTPBearer()

def hash_password(password: str) -> str:
    """
    Хэширует пароль с использованием bcrypt.
    """
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Проверяет соответствие хэшированного пароля и хэша из БД.
    """
    try:
        return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))
    except Exception:
        return False

def create_access_token(user_id: str) -> str:
    """
    Создает JWT токен доступа для пользователя.
    """
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user_id),
        "exp": now + timedelta(hours=TOKEN_EXPIRE_HOURS),
        "iat": now
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def verify_token(credentials: HTTPAuthorizationCredentials = Security(security_bearer)) -> str:
    """
    Проверяет подпись и срок действия JWT токена. Возвращает ID пользователя.
    В случае ошибки выбрасывает HTTPException 401.
    """
    return _verify_token_string(credentials.credentials)


def verify_token_str(token: str) -> str:
    """
    Проверяет JWT токен, переданный как plain string (для WebSocket query-параметра).
    Возвращает ID пользователя.
    """
    return _verify_token_string(token)


def _verify_token_string(token: str) -> str:
    """
    Внутренняя проверка JWT токена.
    """
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        user_id = payload.get("sub")
        if user_id is None:
            raise HTTPException(status_code=401, detail="Неверный токен доступа")
        return user_id
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Срок действия токена истек")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Неверный токен доступа")
