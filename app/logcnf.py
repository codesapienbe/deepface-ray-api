import json
import logging
import os
from datetime import datetime

LOG_FILE = os.getenv("APP_LOG_FILE", "application.log")
LOG_LEVEL = os.getenv("APP_LOG_LEVEL", "INFO").upper()


class JsonFormatter(logging.Formatter):
	def format(self, record: logging.LogRecord) -> str:
		payload = {
			"timestamp": datetime.utcfromtimestamp(record.created).isoformat() + "Z",
			"level": record.levelname,
			"component": record.name,
			"message": record.getMessage(),
		}
		# Optional contextual fields if attached to the record
		for key in ("correlation_id", "user_id", "request_id", "path", "method", "status_code", "latency_ms"):
			value = getattr(record, key, None)
			if value is not None:
				payload[key] = value
		return json.dumps(payload, separators=(",", ":"))


def configure_logging() -> None:
	root = logging.getLogger()
	root.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
	# Clear existing handlers to avoid duplicates
	root.handlers = []
	file_handler = logging.FileHandler(LOG_FILE)
	file_handler.setFormatter(JsonFormatter())
	root.addHandler(file_handler)
	# Also keep console output for local dev
	console = logging.StreamHandler()
	console.setFormatter(JsonFormatter())
	root.addHandler(console)
	# Align common library loggers with root configuration to ensure consistent JSON output
	for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "fastapi", "ray", "asyncio", "access"):
		logger = logging.getLogger(name)
		logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
		# Remove custom handlers so logs propagate to root JSON handlers
		logger.handlers = []
		logger.propagate = True 