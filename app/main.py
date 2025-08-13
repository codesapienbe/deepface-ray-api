import ray
from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from typing import List, Dict, Any, Optional
import logging
from contextlib import asynccontextmanager
import os
import uuid

from .models import (
    VerifyRequest, VerifyResponse, AnalyzeRequest, AnalyzeResponse,
    FindRequest, FindResponse, ExtractEmbeddingRequest, ExtractEmbeddingResponse
)
from .ray_tasks import DeepFaceWorker, get_deepface_models
from .utils import process_uploaded_file
from .auth import auth_router, Role, User
from .security import require_jwt_or_api_key, rate_limit
from .errors import add_exception_handlers
from .signing import add_hmac_signing_middleware
from .reliability import CircuitBreaker, retry_call, log_to_dlq, CircuitOpenError
from .security_headers import add_security_headers

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables for Ray actors
deepface_workers: List[ray.actor.ActorHandle] = []
try:
    num_workers = int(os.getenv("NUM_WORKERS", "4"))
except Exception:
    num_workers = 4

# Circuit breakers per operation
cb_verify = CircuitBreaker(failure_threshold=3, recovery_timeout_sec=30, half_open_max_success=2)
cb_analyze = CircuitBreaker(failure_threshold=3, recovery_timeout_sec=30, half_open_max_success=2)
cb_find = CircuitBreaker(failure_threshold=3, recovery_timeout_sec=30, half_open_max_success=2)
cb_embed = CircuitBreaker(failure_threshold=3, recovery_timeout_sec=30, half_open_max_success=2)


def _parse_csv_env(key: str, default: str) -> List[str]:
    value = os.getenv(key, default)
    items = [item.strip() for item in value.split(",") if item.strip()]
    return items if items else []

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage Ray cluster lifecycle."""
    try:
        # Initialize Ray
        if not ray.is_initialized():
            # Ensure Ray temp dir and dashboard settings are safe for container
            os.environ.setdefault("RAY_DISABLE_DASHBOARD", "1")
            os.environ.setdefault("RAY_TMPDIR", "/tmp/ray")
            try:
                os.makedirs(os.environ["RAY_TMPDIR"], exist_ok=True)
            except Exception as mk_err:
                logger.warning(f"Unable to create RAY_TMPDIR: {mk_err}")
            try:
                ray.init(address="auto", ignore_reinit_error=True, include_dashboard=False)
                logger.info("Ray initialized successfully (address=auto)")
            except Exception as init_err:
                logger.warning(f"Ray auto-connect failed: {init_err}; starting local Ray instance.")
                ray.init(ignore_reinit_error=True, include_dashboard=False)
                logger.info("Ray initialized successfully (local)")

        # Create DeepFace workers
        global deepface_workers
        deepface_workers = [DeepFaceWorker.remote() for _ in range(num_workers)]
        logger.info(f"Created {num_workers} DeepFace workers")

        yield

    except Exception as e:
        logger.error(f"Error during startup: {e}")
        raise
    finally:
        # Cleanup
        if ray.is_initialized():
            ray.shutdown()
            logger.info("Ray shutdown completed")

# Create FastAPI app
app = FastAPI(
    title="DeepFace Ray API",
    description="A scalable API for face recognition and analysis using DeepFace and Ray",
    version="1.0.0",
    lifespan=lifespan
)

# Register centralized error handlers
add_exception_handlers(app)

# Register security headers middleware
add_security_headers(app)

# Register HMAC signing middleware (no-op if disabled)
add_hmac_signing_middleware(app)

# Request ID middleware
@app.middleware("http")
async def add_request_id(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    request.state.request_id = request_id
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response

# Configurable CORS
allowed_origins_env = os.getenv("CORS_ALLOW_ORIGINS", "*").strip()
allow_credentials_env = os.getenv("CORS_ALLOW_CREDENTIALS", "false").lower() == "true"
allowed_methods = _parse_csv_env("CORS_ALLOW_METHODS", "GET,POST,OPTIONS")
allowed_headers = _parse_csv_env("CORS_ALLOW_HEADERS", "Authorization,Content-Type")
exposed_headers = _parse_csv_env("CORS_EXPOSE_HEADERS", "X-Request-ID")
max_age = int(os.getenv("CORS_MAX_AGE", "600"))

if allowed_origins_env == "*":
    allow_origins_cfg = ["*"]
    # Per CORS spec, credentials cannot be used with wildcard origins
    allow_credentials_cfg = False
else:
    allow_origins_cfg = [o.strip() for o in allowed_origins_env.split(",") if o.strip()]
    allow_credentials_cfg = allow_credentials_env

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins_cfg or ["*"],
    allow_credentials=allow_credentials_cfg,
    allow_methods=allowed_methods or ["GET", "POST", "OPTIONS"],
    allow_headers=allowed_headers or ["Authorization", "Content-Type"],
    expose_headers=exposed_headers or ["X-Request-ID"],
    max_age=max_age,
)

# Mount auth routes
app.include_router(auth_router)

def get_worker() -> ray.actor.ActorHandle:
    """Get an available DeepFace worker using round-robin."""
    import random
    return random.choice(deepface_workers)

@app.get("/")
async def root(current_user: User = Depends(require_jwt_or_api_key([Role.VIEWER]))):
    """Root endpoint with API information."""
    return {
        "message": "DeepFace Ray API",
        "version": "1.0.0",
        "ray_initialized": ray.is_initialized(),
        "workers": len(deepface_workers)
    }

@app.get("/health")
async def health_check(current_user: User = Depends(require_jwt_or_api_key([Role.VIEWER]))):
    """Health check endpoint."""
    return {
        "status": "healthy",
        "ray_status": "connected" if ray.is_initialized() else "disconnected",
        "workers_available": len(deepface_workers)
    }

@app.get("/models")
async def get_available_models(
    current_user: User = Depends(require_jwt_or_api_key([Role.VIEWER])),
    _: None = Depends(rate_limit(limit=60, window_seconds=60, scope="models"))
):
    """Get available DeepFace models and backends."""
    try:
        def _call():
            models_future = get_deepface_models.remote()
            return ray.get(models_future)
        models = retry_call(lambda: cb_analyze.call(_call), attempts=3)
        return models
    except CircuitOpenError as e:
        log_to_dlq("models", None, e)
        raise HTTPException(status_code=503, detail="Service temporarily unavailable")
    except Exception as e:
        logger.error(f"Error getting models: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting models: {str(e)}")

@app.post("/verify", response_model=VerifyResponse)
async def verify_faces(
    img1: UploadFile = File(...),
    img2: UploadFile = File(...),
    request: VerifyRequest = VerifyRequest(),
    current_user: User = Depends(require_jwt_or_api_key([Role.OPERATOR, Role.ADMIN])),
    _: None = Depends(rate_limit(limit=30, window_seconds=60, scope="verify"))
):
    """Verify if two faces belong to the same person."""
    try:
        # Process uploaded files
        img1_bytes = await process_uploaded_file(img1)
        img2_bytes = await process_uploaded_file(img2)

        def _call():
            worker = get_worker()
            result_future = worker.verify_faces.remote(
                img1_bytes=img1_bytes,
                img2_bytes=img2_bytes,
                model_name=request.model_name,
                detector_backend=request.detector_backend,
                distance_metric=request.distance_metric,
                enforce_detection=request.enforce_detection,
                align=request.align,
                normalization=request.normalization
            )
            return ray.get(result_future)

        result = retry_call(lambda: cb_verify.call(_call), attempts=2)
        return VerifyResponse(**result)

    except CircuitOpenError as e:
        log_to_dlq("verify", None, e)
        raise HTTPException(status_code=503, detail="Service temporarily unavailable")
    except ValueError as e:
        logger.warning(f"Validation error in face verification: {e}")
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error(f"Error in face verification: {e}")
        raise HTTPException(status_code=500, detail=f"Face verification failed: {str(e)}")

@app.post("/analyze", response_model=AnalyzeResponse)
async def analyze_face(
    image: UploadFile = File(...),
    request: AnalyzeRequest = AnalyzeRequest(),
    current_user: User = Depends(require_jwt_or_api_key([Role.OPERATOR, Role.ADMIN, Role.VIEWER])),
    _: None = Depends(rate_limit(limit=60, window_seconds=60, scope="analyze"))
):
    """Analyze facial attributes (age, gender, emotion, race)."""
    try:
        # Process uploaded file
        img_bytes = await process_uploaded_file(image)

        def _call():
            worker = get_worker()
            result_future = worker.analyze_face.remote(
                img_bytes=img_bytes,
                actions=request.actions,
                model_name=request.model_name,
                detector_backend=request.detector_backend,
                enforce_detection=request.enforce_detection,
                align=request.align,
                silent=request.silent
            )
            return ray.get(result_future)

        result = retry_call(lambda: cb_analyze.call(_call), attempts=2)
        return AnalyzeResponse(results=result)

    except CircuitOpenError as e:
        log_to_dlq("analyze", None, e)
        raise HTTPException(status_code=503, detail="Service temporarily unavailable")
    except ValueError as e:
        logger.warning(f"Validation error in face analysis: {e}")
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error(f"Error in face analysis: {e}")
        raise HTTPException(status_code=500, detail=f"Face analysis failed: {str(e)}")

@app.post("/find", response_model=FindResponse)
async def find_faces(
    query_image: UploadFile = File(...),
    database_images: List[UploadFile] = File(...),
    request: FindRequest = FindRequest(),
    current_user: User = Depends(require_jwt_or_api_key([Role.OPERATOR, Role.ADMIN])),
    _: None = Depends(rate_limit(limit=60, window_seconds=60, scope="find"))
):
    """Find similar faces in a database of images."""
    try:
        # Process query image
        query_bytes = await process_uploaded_file(query_image)

        # Process database images
        db_images = []
        for i, db_img in enumerate(database_images):
            db_bytes = await process_uploaded_file(db_img)
            db_images.append({
                "id": f"image_{i}_{db_img.filename}",
                "image_bytes": db_bytes
            })

        def _call():
            worker = get_worker()
            result_future = worker.find_faces.remote(
                img_bytes=query_bytes,
                db_images=db_images,
                model_name=request.model_name,
                detector_backend=request.detector_backend,
                distance_metric=request.distance_metric,
                enforce_detection=request.enforce_detection,
                align=request.align,
                normalization=request.normalization,
                silent=request.silent
            )
            return ray.get(result_future)

        result = retry_call(lambda: cb_find.call(_call), attempts=2)
        return FindResponse(results=result)

    except CircuitOpenError as e:
        log_to_dlq("find", None, e)
        raise HTTPException(status_code=503, detail="Service temporarily unavailable")
    except ValueError as e:
        logger.warning(f"Validation error in face search: {e}")
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error(f"Error in face search: {e}")
        raise HTTPException(status_code=500, detail=f"Face search failed: {str(e)}")

@app.post("/extract-embedding", response_model=ExtractEmbeddingResponse)
async def extract_face_embedding(
    image: UploadFile = File(...),
    request: ExtractEmbeddingRequest = ExtractEmbeddingRequest(),
    current_user: User = Depends(require_jwt_or_api_key([Role.OPERATOR, Role.ADMIN, Role.VIEWER])),
    _: None = Depends(rate_limit(limit=120, window_seconds=60, scope="embedding"))
):
    """Extract face embedding/representation."""
    try:
        # Process uploaded file
        img_bytes = await process_uploaded_file(image)

        def _call():
            worker = get_worker()
            result_future = worker.extract_embedding.remote(
                img_bytes=img_bytes,
                model_name=request.model_name,
                detector_backend=request.detector_backend,
                enforce_detection=request.enforce_detection,
                align=request.align,
                normalization=request.normalization
            )
            return ray.get(result_future)

        result = retry_call(lambda: cb_embed.call(_call), attempts=2)
        return ExtractEmbeddingResponse(**result)

    except CircuitOpenError as e:
        log_to_dlq("extract-embedding", None, e)
        raise HTTPException(status_code=503, detail="Service temporarily unavailable")
    except ValueError as e:
        logger.warning(f"Validation error in embedding extraction: {e}")
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error(f"Error in embedding extraction: {e}")
        raise HTTPException(status_code=500, detail=f"Embedding extraction failed: {str(e)}")

@app.post("/batch-analyze")
async def batch_analyze_faces(
    images: List[UploadFile] = File(...),
    request: AnalyzeRequest = AnalyzeRequest(),
    current_user: User = Depends(require_jwt_or_api_key([Role.OPERATOR, Role.ADMIN])),
    _: None = Depends(rate_limit(limit=20, window_seconds=60, scope="batch-analyze"))
):
    """Analyze multiple faces in batch."""
    try:
        # Process all images
        tasks = []
        for img in images:
            img_bytes = await process_uploaded_file(img)
            worker = get_worker()
            task = worker.analyze_face.remote(
                img_bytes=img_bytes,
                actions=request.actions,
                model_name=request.model_name,
                detector_backend=request.detector_backend,
                enforce_detection=request.enforce_detection,
                align=request.align,
                silent=request.silent
            )
            tasks.append(task)

        def _call():
            return ray.get(tasks)

        results = retry_call(lambda: cb_analyze.call(_call), attempts=2)

        return {
            "batch_results": [
                {"image_index": i, "analysis": result}
                for i, result in enumerate(results)
            ],
            "total_processed": len(results)
        }

    except CircuitOpenError as e:
        log_to_dlq("batch-analyze", None, e)
        raise HTTPException(status_code=503, detail="Service temporarily unavailable")
    except ValueError as e:
        logger.warning(f"Validation error in batch analysis: {e}")
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error(f"Error in batch analysis: {e}")
        raise HTTPException(status_code=500, detail=f"Batch analysis failed: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        workers=1
    )
