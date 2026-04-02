"""Password hashing, JWT tokens, FastAPI dependencies."""
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Cookie, HTTPException, status
from jose import JWTError, jwt
from passlib.context import CryptContext

from app.auth.firestore_store import StoredUser, get_user_by_username

logger = logging.getLogger(__name__)

SECRET_KEY = os.environ.get("JWT_SECRET_KEY", "mediaexpert-dashboard-secret-change-in-prod")
ALGORITHM = "HS256"
TOKEN_EXPIRE_MINUTES = int(os.environ.get("TOKEN_EXPIRE_MINUTES", "480"))

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(plain: str) -> str:
    return pwd_context.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=TOKEN_EXPIRE_MINUTES))
    to_encode["exp"] = expire
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def decode_token(token: str) -> Optional[dict]:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        return None


def get_current_user(access_token: Optional[str] = None) -> Optional[StoredUser]:
    """Resolve user from JWT cookie. Returns None if invalid."""
    if not access_token:
        return None
    payload = decode_token(access_token)
    if not payload:
        return None
    username: str = payload.get("sub", "")
    if not username:
        return None
    try:
        user = get_user_by_username(username)
    except Exception as e:
        logger.warning("get_current_user: failed to load user %s (%s)", username, e)
        return None
    if not user or not user.is_active:
        return None
    return user


def require_user(access_token: Optional[str] = None) -> StoredUser:
    user = get_current_user(access_token)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    return user


def require_admin(access_token: Optional[str] = None) -> StoredUser:
    user = require_user(access_token)
    if not user.is_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")
    return user


def get_admin_user(access_token: Optional[str] = None) -> StoredUser:
    return require_admin(access_token)
