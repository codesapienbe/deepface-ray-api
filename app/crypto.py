import base64
import os
import hashlib
from typing import Optional
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import secrets

# Key derivation: prefer explicit 32-byte base64 key, else derive from secret via SHA-256
_IMAGE_ENC_KEY_B64 = os.getenv("IMAGE_ENC_KEY_B64")
_IMAGE_ENC_SECRET = os.getenv("IMAGE_ENC_SECRET", "")


def _get_key() -> bytes:
	if _IMAGE_ENC_KEY_B64:
		key = base64.b64decode(_IMAGE_ENC_KEY_B64)
		if len(key) not in (16, 24, 32):
			raise ValueError("IMAGE_ENC_KEY_B64 must decode to 16/24/32 bytes for AES-GCM")
		return key
	if not _IMAGE_ENC_SECRET:
		raise ValueError("IMAGE_ENC_SECRET or IMAGE_ENC_KEY_B64 must be set for encryption")
	# Derive 32-byte key
	return hashlib.sha256(_IMAGE_ENC_SECRET.encode("utf-8")).digest()


def encrypt_bytes(plaintext: bytes, aad: Optional[bytes] = None) -> dict:
	key = _get_key()
	nonce = secrets.token_bytes(12)
	aesgcm = AESGCM(key)
	ciphertext = aesgcm.encrypt(nonce, plaintext, aad)
	return {
		"nonce": base64.b64encode(nonce).decode("ascii"),
		"ciphertext": base64.b64encode(ciphertext).decode("ascii"),
	}


def decrypt_bytes(nonce_b64: str, ciphertext_b64: str, aad: Optional[bytes] = None) -> bytes:
	key = _get_key()
	nonce = base64.b64decode(nonce_b64)
	ciphertext = base64.b64decode(ciphertext_b64)
	aesgcm = AESGCM(key)
	return aesgcm.decrypt(nonce, ciphertext, aad) 