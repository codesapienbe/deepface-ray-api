import base64
import io
import os
from PIL import Image
import numpy as np
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

# Configure PIL to guard against decompression bombs
Image.MAX_IMAGE_PIXELS = 36_000_000

# Image upload constraints
MAX_IMAGE_FILE_SIZE_BYTES = int(os.getenv("MAX_IMAGE_FILE_SIZE_BYTES", str(5 * 1024 * 1024)))  # 5 MB default
ALLOWED_IMAGE_CONTENT_TYPES = {
	"image/jpeg",
	"image/png",
	"image/webp",
}
ALLOWED_IMAGE_FORMATS = {"JPEG", "PNG", "WEBP"}

CHUNK_SIZE = 1024 * 1024  # 1 MB

MALWARE_SCAN_ENABLED = os.getenv("MALWARE_SCAN_ENABLED", "false").lower() == "true"
CLAMAV_HOST = os.getenv("CLAMAV_HOST", "127.0.0.1")
CLAMAV_PORT = int(os.getenv("CLAMAV_PORT", "3310"))

try:
	import clamd  # type: ignore
	except_available_clamd = True
except Exception:
	except_available_clamd = False
	clamd = None  # type: ignore

def image_to_base64(image_bytes: bytes) -> str:
	"""Convert image bytes to base64 string."""
	return base64.b64encode(image_bytes).decode('utf-8')

def base64_to_image_bytes(base64_string: str) -> bytes:
	"""Convert base64 string to image bytes."""
	return base64.b64decode(base64_string)

def validate_image(image_bytes: bytes) -> bool:
	"""Validate if bytes represent a valid image and match allowed formats."""
	try:
		image = Image.open(io.BytesIO(image_bytes))
		image.verify()
		# Re-open to read format reliably after verify
		image = Image.open(io.BytesIO(image_bytes))
		fmt = (image.format or "").upper()
		if fmt not in ALLOWED_IMAGE_FORMATS:
			logger.warning(f"Rejected image with disallowed format: {fmt}")
			return False
		return True
	except Exception:
		return False

def resize_image_if_needed(image_bytes: bytes, max_size: int = 1024) -> bytes:
	"""Resize image if it's too large."""
	try:
		image = Image.open(io.BytesIO(image_bytes))
		if image.mode != 'RGB':
			image = image.convert('RGB')
		if max(image.size) > max_size:
			image.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
		output = io.BytesIO()
		# Always re-encode to strip any malicious payloads/metadata
		image.save(output, format='JPEG', quality=85, optimize=True)
		return output.getvalue()
	except Exception as e:
		logger.error(f"Error resizing image: {e}")
		return image_bytes

def _is_allowed_content_type(content_type: str) -> bool:
	return content_type in ALLOWED_IMAGE_CONTENT_TYPES

def _scan_for_malware(contents: bytes) -> None:
	"""Scan bytes with ClamAV if enabled; raise ValueError on detection or failures when enabled."""
	if not MALWARE_SCAN_ENABLED:
		return
	if not except_available_clamd:
		raise ValueError("Malware scanning enabled but clamd client not available")
	try:
		client = clamd.ClamdNetworkSocket(host=CLAMAV_HOST, port=CLAMAV_PORT)
		result = client.instream(io.BytesIO(contents))
		# Expected result example: {'stream': ('OK', None)} or ('FOUND', 'Eicar-Test-Signature')
		status = result.get('stream', ('UNKNOWN', None))[0]
		if status == 'FOUND':
			raise ValueError("Malware detected in uploaded file")
		elif status != 'OK':
			raise ValueError(f"Malware scan failed with status: {status}")
	except Exception as e:
		logger.error(f"Malware scanning error: {e}")
		raise ValueError("Malware scanning failed")

async def process_uploaded_file(file) -> bytes:
	"""Process uploaded file and return bytes with strict validation and sanitization."""
	# Validate provided content type early (best-effort; still verify content)
	content_type = getattr(file, "content_type", None)
	if not content_type or not _is_allowed_content_type(content_type):
		raise ValueError("Unsupported image content type. Allowed: JPEG, PNG, WEBP")

	# Read in bounded chunks to enforce a hard size limit
	total_read = 0
	chunks = []
	while True:
		chunk = await file.read(CHUNK_SIZE)
		if not chunk:
			break
		total_read += len(chunk)
		if total_read > MAX_IMAGE_FILE_SIZE_BYTES:
			raise ValueError("Image exceeds maximum allowed size")
		chunks.append(chunk)

	contents = b"".join(chunks)

	# Optional malware scan
	_scan_for_malware(contents)

	# Verify image structure and allowed formats
	if not validate_image(contents):
		raise ValueError("Invalid or corrupted image format")

	# Sanitize and downscale if needed, then return bytes
	return resize_image_if_needed(contents)
