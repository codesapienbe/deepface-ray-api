import logging
import time
import random
from typing import Callable, Any, Optional

logger = logging.getLogger(__name__)

class CircuitOpenError(Exception):
	pass

class CircuitBreaker:
	def __init__(self, *, failure_threshold: int, recovery_timeout_sec: int, half_open_max_success: int) -> None:
		self.failure_threshold = max(1, failure_threshold)
		self.recovery_timeout_sec = max(1, recovery_timeout_sec)
		self.half_open_max_success = max(1, half_open_max_success)
		self.state = "closed"  # closed | open | half_open
		self.failure_count = 0
		self.last_failure_ts: float = 0.0
		self.half_open_successes = 0

	def _transition_to_open(self) -> None:
		self.state = "open"
		self.last_failure_ts = time.time()
		logger.warning("Circuit breaker opened")

	def _transition_to_half_open(self) -> None:
		self.state = "half_open"
		self.half_open_successes = 0
		logger.info("Circuit breaker half-open: testing recovery")

	def _transition_to_closed(self) -> None:
		self.state = "closed"
		self.failure_count = 0
		self.half_open_successes = 0
		logger.info("Circuit breaker closed: service recovered")

	def _can_attempt(self) -> bool:
		if self.state == "closed":
			return True
		if self.state == "open":
			if (time.time() - self.last_failure_ts) >= self.recovery_timeout_sec:
				self._transition_to_half_open()
				return True
			return False
		if self.state == "half_open":
			return True
		return False

	def on_success(self) -> None:
		if self.state == "half_open":
			self.half_open_successes += 1
			if self.half_open_successes >= self.half_open_max_success:
				self._transition_to_closed()
				return
		# In closed, keep closed

	def on_failure(self) -> None:
		if self.state in ("closed", "half_open"):
			self.failure_count += 1
			if self.failure_count >= self.failure_threshold:
				self._transition_to_open()
				return
			# Stay in half_open/closed but count failures

	def call(self, func: Callable[[], Any]) -> Any:
		if not self._can_attempt():
			raise CircuitOpenError("Circuit is open; rejecting call")
		try:
			result = func()
			self.on_success()
			return result
		except Exception:
			self.on_failure()
			raise

def retry_call(func: Callable[[], Any], *, attempts: int = 3, backoff_ms: int = 100, factor: float = 2.0, max_backoff_ms: int = 2000, jitter_ms: int = 100) -> Any:
	last_exc: Optional[Exception] = None
	for i in range(attempts):
		try:
			return func()
		except Exception as e:
			last_exc = e
			if i == attempts - 1:
				break
			delay = min(int(backoff_ms * (factor ** i)), max_backoff_ms)
			delay += random.randint(0, max(0, jitter_ms))
			time.sleep(delay / 1000.0)
	if last_exc is not None:
		raise last_exc
	return None

def log_to_dlq(scope: str, request_id: Optional[str], error: Exception, metadata: Optional[dict] = None) -> None:
	payload = {
		"scope": scope,
		"request_id": request_id,
		"error": str(error),
		"metadata": metadata or {},
	}
	logger.error(f"DLQ event: {payload}") 