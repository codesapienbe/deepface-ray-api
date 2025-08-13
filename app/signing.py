import base64
import hmac
import hashlib
import os
import time
from typing import Optional

from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import Response

SIGNING_ENABLED = os.getenv("SIGNING_ENABLED", "false").lower() == "true"
SIGNING_SECRET = os.getenv("SIGNING_SECRET", "")
SIGNING_TOLERANCE_SECONDS = int(os.getenv("SIGNING_TOLERANCE_SECONDS", "300"))
SIGNING_SIGN_RESPONSES = os.getenv("SIGNING_SIGN_RESPONSES", "true").lower() == "true"


def _compute_signature(secret: str, payload: bytes, timestamp: str) -> str:
	message = timestamp.encode("utf-8") + b"." + payload
	digest = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).digest()
	return base64.b64encode(digest).decode("ascii")


def _response_body_bytes(response: Response) -> bytes:
	body = getattr(response, "body", None)
	if isinstance(body, (bytes, bytearray)):
		return bytes(body)
	return b""


def add_hmac_signing_middleware(app: FastAPI) -> None:
	if not SIGNING_ENABLED or not SIGNING_SECRET:
		return

	@app.middleware("http")
	async def hmac_signing_middleware(request: Request, call_next):
		# Allow unauthenticated access to API documentation and OpenAPI schema
		path = request.url.path
		if path.startswith("/docs") or path.startswith("/redoc") or path.startswith("/openapi") or path == "/favicon.ico":
			return await call_next(request)

		# Read and buffer the body for downstream access
		raw_body: bytes = await request.body()

		async def receive() -> dict:
			return {"type": "http.request", "body": raw_body, "more_body": False}

		# Re-inject the body so route handlers can read it
		setattr(request, "_receive", receive)

		req_timestamp = request.headers.get("X-Timestamp")
		req_signature = request.headers.get("X-Signature")

		if not req_timestamp or not req_signature:
			raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing signature headers")

		# Validate timestamp
		try:
			ts_val = int(req_timestamp)
		except Exception:
			raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid timestamp header")
		now = int(time.time())
		if abs(now - ts_val) > SIGNING_TOLERANCE_SECONDS:
			raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Stale request")

		# Validate signature
		expected = _compute_signature(SIGNING_SECRET, raw_body, req_timestamp)
		if not hmac.compare_digest(expected, req_signature):
			raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid signature")

		# Proceed
		response = await call_next(request)

		if SIGNING_SIGN_RESPONSES:
			res_body_bytes = _response_body_bytes(response)
			res_timestamp = str(int(time.time()))
			res_signature = _compute_signature(SIGNING_SECRET, res_body_bytes, res_timestamp)
			response.headers["X-Response-Timestamp"] = res_timestamp
			response.headers["X-Response-Signature"] = res_signature

		return response 