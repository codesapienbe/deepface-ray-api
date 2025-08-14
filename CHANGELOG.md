# Changelog

[1.8.4] - 2025-08-13

### Changed - P0 Containerization Tooling
- Ensured all `make` targets run via Docker with no local `uv` usage.
  - Removed host-side `uv run` calls from `clean` and `stop`.
  - `make test` now runs the test client inside the image without `uv`, mounting the workspace.
  - `make serve` now runs the containerized API in the foreground.
  - `make deps` delegates to `make install` (container image build).
- Added `requests` to application dependencies to support the containerized test client.
- Relaxed CSP for `/docs` and `/redoc` in `app/secheaders.py` to allow Swagger/ReDoc assets (JS/CSS/fonts) so API docs render correctly.
- Fixed multipart form parsing for options on `/verify`, `/analyze`, `/batch-analyze`, and `/extract-embedding` by binding request models via `Depends(Model.as_form)`. This resolves 422 errors like `type_error.dict` when sending files with form fields.
- Hardened image decoding in `app/tasks.py` to use OpenCV for bytes-to-array conversion with a PIL fallback, preventing DeepFace errors like `AttributeError: 'tuple' object has no attribute 'shape'`.
- Resource tuning for low-memory nodes:
  - Docker entrypoint no longer starts a Ray head; app uses Ray local mode to reduce overhead.
  - Reduced default `RAY_object_store_memory` to 256MB; capped threads via `OMP_NUM_THREADS=1`, `OPENBLAS_NUM_THREADS=1`, `MKL_NUM_THREADS=1`, and TensorFlow thread envs.
  - Default `NUM_WORKERS` set to 1; default model set to `SFace` for lighter memory footprint.
- DeepFace fallbacks updated to avoid local I/O entirely: use in-memory base64 data URIs for retry paths in `verify_faces` and `find_faces`.
- Added backend abstraction and factory selection; introduced Kafka worker backend (publish/subscribe via request/response topics with correlation IDs). New provider `kafka` selectable via `WORKER_PROVIDER`.

[1.8.3] - 2025-08-13

### Changed - P0 Containerization
- Consolidated Ray and FastAPI into a single Docker image and entrypoint.
  - Container starts a local Ray head and then launches the API.
  - No docker-compose required for default single-node deployment.
- Updated `Makefile run` to use `docker run` for a single-container workflow, aligning dev and prod.

[1.8.2] - 2025-08-13

### Changed - P0 Startup Reliability
- `make serve` now starts a local Ray head automatically before launching the API.
  - Uses `uv run ray start --head` to ensure it runs in the project virtual environment.
  - `stop` and `clean` targets now issue `ray stop` to gracefully shut down the local cluster.
- macOS Apple Silicon fix: switched to `tensorflow-macos==2.15.0` via environment markers to resolve `ModuleNotFoundError: No module named 'tensorflow'` during startup.

[1.8.1] - 2025-08-13

### Added - P1 Developer Tooling
- Added `pyproject.toml` to adopt uv for dependency management.
  - Migrated pinned dependencies from `requirements.txt` to `[project.dependencies]`.
  - Created `dev` dependency group for tooling (`ruff`).
  - Compatible with `uv sync` and `uv run` workflows.

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