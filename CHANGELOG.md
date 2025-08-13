# Changelog

[1.8.0] - 2025-08-13

### Added - P0 Ray Operations
- Round-robin worker selection for better load distribution.
- New endpoint `/ray/health` to report per-worker health via ping.

[1.7.0] - 2025-08-13

### Added - P0 Input Validation Enhancements
- Stronger Pydantic validation: added enums for actions and normalization.
- Enforced maximum list sizes via env: `MAX_DB_IMAGES` and `MAX_BATCH_IMAGES` with 422 responses on overflow.

[1.6.0] - 2025-08-13

### Added - P0 Input Security
- Optional ClamAV malware scanning for uploaded images (`MALWARE_SCAN_ENABLED`, `CLAMAV_HOST`, `CLAMAV_PORT`).
- Stricter image format validation against allowlist (JPEG, PNG, WEBP) based on actual file format.

[1.5.0] - 2025-08-13

### Added - P0 Transport Security
- Optional TLS support for Uvicorn via `run.py` with env configuration.
  - Env: TLS_ENABLED, TLS_CERTFILE, TLS_KEYFILE, TLS_CA_CERTS.
  - Dockerfile updated to use `python run.py` as entrypoint for TLS.

[1.4.0] - 2025-08-13

### Added - P0 Data Integrity
- HMAC-based request verification and response signing middleware (env-controlled).
  - Headers: X-Timestamp, X-Signature for requests; X-Response-Timestamp, X-Response-Signature for responses.
  - Config: SIGNING_ENABLED, SIGNING_SECRET, SIGNING_TOLERANCE_SECONDS, SIGNING_SIGN_RESPONSES.

[1.3.0] - 2025-08-13

### Added - P0 Service Auth & Throttling
- API key authentication for service-to-service calls with role mapping (`X-API-Key` or `Authorization: ApiKey <key>`).
- Rate limiting dependency with Redis backend and in-memory fallback; per-endpoint limits added.
- Either JWT or API key can authorize access depending on environment flags.

[1.2.0] - 2025-08-13

### Added - P0 Authentication Enhancements
- Implemented JWT refresh tokens and refresh endpoint `/auth/refresh`.
  - Separate secrets/expirations for access and refresh tokens.
  - Backward compatible response with additional refresh fields.
- Configurable CORS via environment variables with secure defaults.

[1.1.1] - 2025-08-13

### Changed - P0 Stability Improvements
- Ray initialization hardened: dashboard disabled to prevent agent crashes in constrained environments.
- Docker Compose updated with shm_size=2g to improve Ray object store performance and avoid /dev/shm warnings.
- NUM_WORKERS now configurable via environment variable with safe fallback.
- Dependency pinned: added bcrypt==4.0.1 to resolve passlib bcrypt backend warning.

[1.1.0] - 2025-08-13

### Added - P0 Security Authentication
- JWT access token authentication with role-based access control (RBAC).
  - New `app/auth.py` with token issuance endpoint `/auth/token`.
  - Roles: admin, operator, viewer; helper dependency `require_roles`.
  - Optional enforcement via `AUTH_ENABLED` env var (default: false) to preserve existing behavior.
  - Expiring tokens (15 minutes by default) using HS256.
  - Security: Authorization checks integrated into endpoints without changing responses.

[1.0.1] - 2025-08-13

### Added - P0 Security & Input Validation
- Strengthened image upload validation and sanitization across all endpoints.
  - Enforced max image size limit with chunked reads (default 5 MB; configurable via `MAX_IMAGE_FILE_SIZE_BYTES`).
  - Restricted allowed content types to JPEG, PNG, and WEBP.
  - Enabled PIL decompression bomb protection and re-encoding to sanitized JPEG.
  - Standardized 422 responses for validation errors.
  - Centralized processing in `app/utils.py` used by all image endpoints.
  - Performance: Negligible overhead with 1 MB chunk size; re-encoding optimized.
  - Security: Reduces risk of DoS via oversized files and prevents malformed image payload exploits.

[1.0.2] - 2025-08-13

### Changed - P0 Deployment Reliability
- Docker Compose: removed hard dependency on `ray-head` and defaulted `RAY_ADDRESS` to `auto` for single-container startup.
  - Optional `ray-head` service is now gated behind the `multi-node` profile.
  - Updated README with compose usage for single-node and multi-node modes. 