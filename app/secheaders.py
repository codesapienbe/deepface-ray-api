import os
from fastapi import FastAPI, Request

TLS_ENABLED = os.getenv("TLS_ENABLED", "false").lower() == "true"


def add_security_headers(app: FastAPI) -> None:
	@app.middleware("http")
	async def security_headers_middleware(request: Request, call_next):
		response = await call_next(request)
		# Basic hardening headers for APIs
		response.headers.setdefault("X-Content-Type-Options", "nosniff")
		response.headers.setdefault("X-Frame-Options", "DENY")
		response.headers.setdefault("Referrer-Policy", "no-referrer")
		# Strict CSP for APIs (no resources permitted)
		response.headers.setdefault("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'; base-uri 'none'; block-all-mixed-content")
		# Permissions Policy (lock down powerful features)
		response.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
		# HSTS only when TLS is enabled
		if TLS_ENABLED:
			response.headers.setdefault("Strict-Transport-Security", "max-age=63072000; includeSubDomains; preload")
		return response 