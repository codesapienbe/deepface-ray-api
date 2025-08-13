from typing import Any, Dict
import logging
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel

logger = logging.getLogger(__name__)

class ErrorResponse(BaseModel):
    error: str
    detail: Any
    request_id: str | None = None

def _get_request_id(request: Request) -> str | None:
    return getattr(request.state, "request_id", None) or request.headers.get("X-Request-ID")

def add_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(RequestValidationError)
    async def handle_request_validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        request_id = _get_request_id(request)
        logger.warning(f"Request validation failed: {exc}")
        body = ErrorResponse(error="validation_error", detail=exc.errors(), request_id=request_id)
        return JSONResponse(status_code=422, content=body.dict())

    @app.exception_handler(HTTPException)
    async def handle_http_exception(request: Request, exc: HTTPException) -> JSONResponse:
        request_id = _get_request_id(request)
        logger.info(f"HTTP error {exc.status_code}: {exc.detail}")
        body = ErrorResponse(error="http_error", detail=exc.detail, request_id=request_id)
        return JSONResponse(status_code=exc.status_code, content=body.dict())

    @app.exception_handler(Exception)
    async def handle_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
        request_id = _get_request_id(request)
        logger.error(f"Unexpected error: {exc}", exc_info=True)
        body = ErrorResponse(error="internal_server_error", detail="Internal server error", request_id=request_id)
        return JSONResponse(status_code=500, content=body.dict()) 