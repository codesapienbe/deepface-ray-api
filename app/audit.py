import logging
from typing import Any, Dict, Optional

_audit_logger = logging.getLogger("audit")

def log_audit(event: str, user_id: Optional[str], request_id: Optional[str], metadata: Optional[Dict[str, Any]] = None) -> None:
	payload = {
		"event": event,
		"user_id": user_id,
		"request_id": request_id,
	}
	if metadata:
		payload.update({f"meta_{k}": v for k, v in metadata.items()})
	_audit_logger.info("audit_event", extra=payload) 