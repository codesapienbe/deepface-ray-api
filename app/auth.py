import os
import logging
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Callable, List, Optional, Awaitable
import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel
from jose import JWTError, jwt
from passlib.context import CryptContext

logger = logging.getLogger(__name__)

auth_router = APIRouter()
security = HTTPBearer(auto_error=False)
password_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Environment-driven settings
AUTH_ENABLED = os.getenv("AUTH_ENABLED", "false").lower() == "true"
JWT_SECRET = os.getenv("JWT_SECRET", "change-this-in-prod")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
JWT_EXPIRE_MINUTES = int(os.getenv("JWT_EXPIRE_MINUTES", "15"))
REFRESH_TOKEN_SECRET = os.getenv("REFRESH_TOKEN_SECRET", JWT_SECRET)
REFRESH_TOKEN_EXPIRE_MINUTES = int(os.getenv("REFRESH_TOKEN_EXPIRE_MINUTES", "10080"))  # 7 days

class Role(str, Enum):
	ADMIN = "admin"
	OPERATOR = "operator"
	VIEWER = "viewer"

class User(BaseModel):
	username: str
	role: Role

# Minimal in-memory user store for demo purposes only
# In production, integrate with a real user directory/IdP
_IN_MEMORY_USERS = {
	"admin": {
		"password_hash": password_context.hash("admin123"),
		"role": Role.ADMIN,
	},
	"operator": {
		"password_hash": password_context.hash("operator123"),
		"role": Role.OPERATOR,
	},
	"viewer": {
		"password_hash": password_context.hash("viewer123"),
		"role": Role.VIEWER,
	},
}

class TokenResponse(BaseModel):
	access_token: str
	refresh_token: Optional[str] = None
	token_type: str = "bearer"
	expires_in: int
	refresh_expires_in: Optional[int] = None

class LoginRequest(BaseModel):
	username: str
	password: str

class RefreshRequest(BaseModel):
	refresh_token: str

def _now_ts() -> int:
	return int(datetime.now(timezone.utc).timestamp())

def _create_access_token(*, username: str, role: Role, expires_delta: Optional[timedelta] = None) -> str:
	expire = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=JWT_EXPIRE_MINUTES))
	payload = {
		"sub": username,
		"role": role.value,
		"type": "access",
		"jti": str(uuid.uuid4()),
		"exp": int(expire.timestamp()),
		"iat": _now_ts(),
	}
	token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
	return token

def _create_refresh_token(*, username: str, role: Role, expires_delta: Optional[timedelta] = None) -> str:
	expire = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=REFRESH_TOKEN_EXPIRE_MINUTES))
	payload = {
		"sub": username,
		"role": role.value,
		"type": "refresh",
		"jti": str(uuid.uuid4()),
		"exp": int(expire.timestamp()),
		"iat": _now_ts(),
	}
	return jwt.encode(payload, REFRESH_TOKEN_SECRET, algorithm=JWT_ALGORITHM)

@auth_router.post("/auth/token", response_model=TokenResponse)
async def issue_token(login: LoginRequest) -> TokenResponse:
	if not AUTH_ENABLED:
		# To avoid surprise in production, enforce explicit enablement
		raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Authentication is disabled on this server")

	user_record = _IN_MEMORY_USERS.get(login.username)
	if not user_record or not password_context.verify(login.password, user_record["password_hash"]):
		logger.warning("Auth failed: invalid credentials")
		raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

	role: Role = user_record["role"]
	access_token = _create_access_token(username=login.username, role=role)
	refresh_token = _create_refresh_token(username=login.username, role=role)
	logger.info(f"Auth success for user={login.username} role={role}")
	return TokenResponse(
		access_token=access_token,
		refresh_token=refresh_token,
		expires_in=JWT_EXPIRE_MINUTES * 60,
		refresh_expires_in=REFRESH_TOKEN_EXPIRE_MINUTES * 60,
	)

@auth_router.post("/auth/refresh", response_model=TokenResponse)
async def refresh_access_token(body: RefreshRequest) -> TokenResponse:
	if not AUTH_ENABLED:
		raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Authentication is disabled on this server")

	try:
		payload = jwt.decode(body.refresh_token, REFRESH_TOKEN_SECRET, algorithms=[JWT_ALGORITHM])
		token_type: Optional[str] = payload.get("type")
		username_opt: Optional[str] = payload.get("sub")
		role_str_opt: Optional[str] = payload.get("role")
		if token_type != "refresh" or not username_opt or not role_str_opt:
			raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid refresh token")
		role = Role(role_str_opt)
		new_access = _create_access_token(username=username_opt, role=role)
		new_refresh = _create_refresh_token(username=username_opt, role=role)
		return TokenResponse(
			access_token=new_access,
			refresh_token=new_refresh,
			expires_in=JWT_EXPIRE_MINUTES * 60,
			refresh_expires_in=REFRESH_TOKEN_EXPIRE_MINUTES * 60,
		)
	except JWTError as e:
		logger.warning(f"Refresh token validation failed: {e}")
		raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired refresh token")

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> User:
	if not AUTH_ENABLED:
		# Anonymous access when auth disabled; treated as viewer
		return User(username="anonymous", role=Role.VIEWER)

	if credentials is None or not credentials.credentials:
		raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing bearer token")

	token = credentials.credentials
	try:
		payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
		username_opt: Optional[str] = payload.get("sub")
		role_str_opt: Optional[str] = payload.get("role")
		type_claim: Optional[str] = payload.get("type")
		if type_claim != "access" or not username_opt or not role_str_opt:
			raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload")
		return User(username=username_opt, role=Role(role_str_opt))
	except JWTError as e:
		logger.warning(f"Token validation failed: {e}")
		raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token")

def require_roles(allowed_roles: List[Role]) -> Callable[..., Awaitable[User]]:
	async def dependency(user: User = Depends(get_current_user)) -> User:
		if not AUTH_ENABLED:
			return user
		if user.role not in allowed_roles:
			raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permissions")
		return user
	return dependency 