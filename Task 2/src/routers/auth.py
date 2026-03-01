import jwt
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from passlib.context import CryptContext
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.database import get_async_session
from src.models.db import UserDB
from src.models.generated import UserRegister, UserLogin, TokenResponse, RefreshRequest

router = APIRouter()
security = HTTPBearer()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

SECRET_KEY = "marketplace_secret"
ALGORITHM = "HS256"
ACCESS_TOKEN_MINUTES = 30
REFRESH_TOKEN_DAYS = 30


def error_resp(code: int, err_code: str, msg: str):
    return JSONResponse(status_code=code, content={"error_code": err_code, "message": msg})


def create_token(data: dict, expires_delta: timedelta):
    to_encode = data.copy()
    to_encode.update({"exp": datetime.now(timezone.utc) + expires_delta})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


class RoleChecker:
    def __init__(self, allowed_roles: list):
        self.allowed_roles = allowed_roles

    def __call__(self, user: dict = Depends(get_current_user)):
        if not user:
            return None
        if user.get("role") not in self.allowed_roles:
            return False
        return user


@router.post("/register", status_code=status.HTTP_201_CREATED)
async def register(user_in: UserRegister, session: AsyncSession = Depends(get_async_session)):
    query = select(UserDB).where(UserDB.username == user_in.username)
    if (await session.execute(query)).scalars().first():
        return error_resp(400, "USER_EXISTS", "Username already exists")

    hashed_pw = pwd_context.hash(user_in.password)
    new_user = UserDB(username=user_in.username, password_hash=hashed_pw, role=user_in.role.value)
    session.add(new_user)
    await session.commit()
    return {"status": "success"}


@router.post("/login", response_model=TokenResponse)
async def login(user_in: UserLogin, session: AsyncSession = Depends(get_async_session)):
    query = select(UserDB).where(UserDB.username == user_in.username)
    user = (await session.execute(query)).scalars().first()

    if not user or not pwd_context.verify(user_in.password, user.password_hash):
        return error_resp(401, "AUTH_FAILED", "Incorrect username or password")

    access = create_token({"sub": str(user.id), "role": user.role}, timedelta(minutes=ACCESS_TOKEN_MINUTES))
    refresh = create_token({"sub": str(user.id), "type": "refresh"}, timedelta(days=REFRESH_TOKEN_DAYS))
    return TokenResponse(access_token=access, refresh_token=refresh)


@router.post("/refresh", response_model=TokenResponse)
async def refresh(req: RefreshRequest, session: AsyncSession = Depends(get_async_session)):
    try:
        payload = jwt.decode(req.refresh_token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload.get("type") != "refresh":
            return error_resp(401, "REFRESH_TOKEN_INVALID", "Invalid token type")

        user = (await session.execute(select(UserDB).where(UserDB.id == payload.get("sub")))).scalars().first()
        if not user:
            return error_resp(401, "REFRESH_TOKEN_INVALID", "User not found")

        access = create_token({"sub": str(user.id), "role": user.role}, timedelta(minutes=ACCESS_TOKEN_MINUTES))
        refresh_token = create_token({"sub": str(user.id), "type": "refresh"}, timedelta(days=REFRESH_TOKEN_DAYS))
        return TokenResponse(access_token=access, refresh_token=refresh_token)
    except Exception:
        return error_resp(401, "REFRESH_TOKEN_INVALID", "Invalid or expired refresh token")