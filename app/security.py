import os
import logging
import time
from collections import deque
from typing import Callable, Deque, Dict, Optional, Tuple, List

from fastapi import Depends, HTTPException, Request, status

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    redis = None

from jose import JWTError, jwt

from .auth import Role, User, AUTH_ENABLED, JWT_SECRET, JWT_ALGORITHM

logger = logging.getLogger(__name__)

API_KEY_ENABLED = os.getenv("API_KEY_ENABLED", "false").lower() == "true"
API_KEYS_ENV = os.getenv("API_KEYS", "")  # format: key[:role],key2[:role]
RATE_LIMIT_ENABLED = os.getenv("RATE_LIMIT_ENABLED", "false").lower() == "true"
REDIS_URL = os.getenv("REDIS_URL", "")

# Parse API keys into mapping key -> Role

def _parse_api_keys(env_value: str) -> Dict[str, Role]:
    mapping: Dict[str, Role] = {}
    for raw in [x.strip() for x in env_value.split(",") if x.strip()]:
        if ":" in raw:
            key, role_str = raw.split(":", 1)
            role = Role(role_str.strip()) if role_str.strip() in {r.value for r in Role} else Role.VIEWER
            mapping[key.strip()] = role
        else:
            mapping[raw] = Role.VIEWER
    return mapping

API_KEY_TO_ROLE: Dict[str, Role] = _parse_api_keys(API_KEYS_ENV)


def _extract_api_key(request: Request) -> Optional[str]:
    # Prefer X-API-Key header
    api_key = request.headers.get("X-API-Key")
    if api_key:
        return api_key.strip()
    # Support Authorization: ApiKey <key>
    auth = request.headers.get("Authorization", "")
    if auth.lower().startswith("apikey "):
        return auth.split(" ", 1)[1].strip()
    return None


def _extract_bearer_token(request: Request) -> Optional[str]:
    auth = request.headers.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    return None


def require_jwt_or_api_key(allowed_roles: List[Role]) -> Callable[[Request], User]:
    async def dependency(request: Request) -> User:
        # Allow anonymous if both mechanisms disabled
        if not AUTH_ENABLED and not API_KEY_ENABLED:
            return User(username="anonymous", role=Role.VIEWER)

        # Try JWT first if enabled and present
        bearer = _extract_bearer_token(request) if AUTH_ENABLED else None
        if bearer:
            try:
                payload = jwt.decode(bearer, JWT_SECRET, algorithms=[JWT_ALGORITHM])
                token_type: Optional[str] = payload.get("type")
                username_opt: Optional[str] = payload.get("sub")
                role_str_opt: Optional[str] = payload.get("role")
                if token_type == "access" and username_opt and role_str_opt:
                    user = User(username=username_opt, role=Role(role_str_opt))
                    if user.role in allowed_roles:
                        return user
                    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permissions")
            except JWTError as e:
                logger.warning(f"JWT validation failed: {e}")
                # fallthrough to API key

        # Try API Key if enabled
        if API_KEY_ENABLED:
            api_key = _extract_api_key(request)
            if api_key and api_key in API_KEY_TO_ROLE:
                role = API_KEY_TO_ROLE[api_key]
                if role in allowed_roles:
                    return User(username="service", role=role)
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permissions")

        # If we reach here, neither auth succeeded
        if AUTH_ENABLED:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required")
        # If only API key enabled
        if API_KEY_ENABLED:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="API key required")
        # Default deny
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

    return dependency


class _InMemoryLimiter:
    def __init__(self) -> None:
        self.bucket: Dict[str, Deque[float]] = {}

    def allow(self, key: str, limit: int, window_sec: int) -> Tuple[bool, int]:
        now = time.time()
        dq = self.bucket.setdefault(key, deque())
        # purge old
        while dq and now - dq[0] > window_sec:
            dq.popleft()
        if len(dq) >= limit:
            retry_after = int(window_sec - (now - dq[0])) if dq else window_sec
            return False, max(retry_after, 1)
        dq.append(now)
        return True, 0


class _RedisLimiter:
    def __init__(self, client: "redis.Redis") -> None:
        self.client = client

    def allow(self, key: str, limit: int, window_sec: int) -> Tuple[bool, int]:
        # Sliding window via simple fixed window approximation using INCR with EXPIRE
        pipe = self.client.pipeline()
        pipe.incr(key, 1)
        pipe.expire(key, window_sec)
        count, _ = pipe.execute()
        if int(count) > limit:
            ttl = self.client.ttl(key)
            retry_after = int(ttl) if isinstance(ttl, int) and ttl > 0 else window_sec
            return False, retry_after
        return True, 0


_limiter: Optional[object] = None
if REDIS_URL and redis is not None:
    try:
        _redis_client = redis.Redis.from_url(REDIS_URL)
        # Test connection
        _redis_client.ping()
        _limiter = _RedisLimiter(_redis_client)
        logger.info("Rate limiter: using Redis backend")
    except Exception as e:  # pragma: no cover
        logger.warning(f"Failed to init Redis limiter, falling back to in-memory: {e}")
        _limiter = _InMemoryLimiter()
else:
    _limiter = _InMemoryLimiter()


def rate_limit(limit: int, window_seconds: int, scope: str) -> Callable[[Request], None]:
    async def dependency(request: Request) -> None:
        if not RATE_LIMIT_ENABLED:
            return
        identifier = _extract_api_key(request) or _extract_bearer_token(request) or (request.client.host if request.client else "unknown")
        key = f"rl:{scope}:{identifier}"
        assert _limiter is not None
        allowed, retry_after = _limiter.allow(key, limit, window_seconds)
        if not allowed:
            raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Rate limit exceeded", headers={"Retry-After": str(retry_after)})
    return dependency 