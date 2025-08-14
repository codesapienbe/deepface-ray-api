import os
import ray
import numpy as np
from deepface import DeepFace
from PIL import Image
import io
import base64
from typing import List, Dict, Any, Optional
import logging
import cv2
from celery import Celery
import uuid
import time
from threading import RLock
import json
from .models import VerifyJob, AnalyzeJob, FindJob, ExtractEmbeddingJob

# Configure logging (root handlers configured in app.logcnf)
logger = logging.getLogger(__name__)

# Celery application configured to run tasks eagerly by default (no external broker required)
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "memory://")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "cache+memory://")
celery_app = Celery("deepface", broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)
# Run tasks in-process unless explicitly disabled
celery_app.conf.task_always_eager = os.getenv("CELERY_EAGER", "true").lower() == "true"
celery_app.conf.task_ignore_result = False

# Simple in-memory task registry
_TASKS: Dict[str, Dict[str, Any]] = {}
_TASKS_LOCK = RLock()


def _now_ts() -> float:
	return time.time()


def register_ray_ref(kind: str, ref: "ray.ObjectRef") -> str:
	task_id = f"ray-{uuid.uuid4()}"
	with _TASKS_LOCK:
		_TASKS[task_id] = {
			"kind": kind,
			"backend": "ray",
			"status": "pending",
			"ref": ref,
			"created_at": _now_ts(),
			"updated_at": _now_ts(),
		}
	return task_id


def register_celery_task(kind: str, celery_task_id: str) -> str:
	task_id = f"celery-{celery_task_id}"
	with _TASKS_LOCK:
		_TASKS[task_id] = {
			"kind": kind,
			"backend": "celery",
			"status": "pending",
			"celery_id": celery_task_id,
			"created_at": _now_ts(),
			"updated_at": _now_ts(),
		}
	return task_id


def register_local_result(kind: str, result: Any) -> str:
	task_id = f"local-{uuid.uuid4()}"
	with _TASKS_LOCK:
		_TASKS[task_id] = {
			"kind": kind,
			"backend": "local",
			"status": "success",
			"result": result,
			"created_at": _now_ts(),
			"updated_at": _now_ts(),
		}
	return task_id

# Kafka optional support
try:
	from kafka import KafkaProducer, KafkaConsumer  # type: ignore
	_KAFKA_AVAILABLE = True
except Exception:
	_KAFKA_AVAILABLE = False
	KafkaProducer = None  # type: ignore
	KafkaConsumer = None  # type: ignore

_KAFKA_PRODUCER = None
_KAFKA_CONSUMER = None


def _get_kafka_bootstrap_servers() -> List[str]:
	servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
	return [s.strip() for s in servers.split(",") if s.strip()]


def _get_kafka_producer():
	global _KAFKA_PRODUCER
	if _KAFKA_PRODUCER is not None:
		return _KAFKA_PRODUCER
	if not _KAFKA_AVAILABLE:
		raise RuntimeError("Kafka not available")
	_KAFKA_PRODUCER = KafkaProducer(
		bootstrap_servers=_get_kafka_bootstrap_servers(),
		value_serializer=lambda v: json.dumps(v).encode("utf-8"),
		key_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
		acks='all',
		retries=0,
		request_timeout_ms=int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "2000")),
		max_block_ms=int(os.getenv("KAFKA_MAX_BLOCK_MS", "2000")),
		api_version_auto_timeout_ms=int(os.getenv("KAFKA_API_VERSION_TIMEOUT_MS", "2000")),
	)
	return _KAFKA_PRODUCER


def _get_kafka_consumer():
	global _KAFKA_CONSUMER
	if _KAFKA_CONSUMER is not None:
		return _KAFKA_CONSUMER
	if not _KAFKA_AVAILABLE:
		raise RuntimeError("Kafka not available")
	group_id = os.getenv("KAFKA_GROUP_ID", "deepface-api")
	response_topic = os.getenv("KAFKA_RESPONSE_TOPIC", "deepface.responses")
	_KAFKA_CONSUMER = KafkaConsumer(
		response_topic,
		bootstrap_servers=_get_kafka_bootstrap_servers(),
		group_id=group_id,
		auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
		value_deserializer=lambda v: json.loads(v.decode("utf-8")),
		request_timeout_ms=int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "2000")),
		api_version_auto_timeout_ms=int(os.getenv("KAFKA_API_VERSION_TIMEOUT_MS", "2000")),
		session_timeout_ms=int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "3000")),
		max_poll_interval_ms=int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "3000")),
	)
	return _KAFKA_CONSUMER


def register_kafka_task(kind: str, task_id: str) -> str:
	with _TASKS_LOCK:
		_TASKS[task_id] = {
			"kind": kind,
			"backend": "kafka",
			"status": "pending",
			"created_at": _now_ts(),
			"updated_at": _now_ts(),
		}
	return task_id


def get_task_status(task_id: str) -> Dict[str, Any]:
	with _TASKS_LOCK:
		entry = _TASKS.get(task_id)
	if entry is None:
		return {"task_id": task_id, "status": "not_found"}

	backend = entry.get("backend")
	status = entry.get("status", "pending")

	if backend == "ray":
		ref = entry.get("ref")
		if ref is None:
			return {"task_id": task_id, "backend": backend, "status": status}
		# Non-blocking check
		ready, _ = ray.wait([ref], timeout=0.0)
		if ready:
			try:
				result = ray.get(ref)
				entry.update({"status": "success", "result": result, "updated_at": _now_ts()})
			except Exception as e:
				entry.update({"status": "error", "error": str(e), "updated_at": _now_ts()})
			with _TASKS_LOCK:
				_TASKS[task_id] = entry
		return {"task_id": task_id, "backend": backend, "status": entry["status"], "result": entry.get("result"), "error": entry.get("error")}

	if backend == "celery":
		celery_id = entry.get("celery_id")
		if not celery_id:
			return {"task_id": task_id, "backend": backend, "status": status}
		ar = celery_app.AsyncResult(celery_id)
		if ar.successful():
			try:
				result = ar.get(propagate=False)
				entry.update({"status": "success", "result": result, "updated_at": _now_ts()})
			except Exception as e:
				entry.update({"status": "error", "error": str(e), "updated_at": _now_ts()})
			with _TASKS_LOCK:
				_TASKS[task_id] = entry
		elif ar.failed():
			entry.update({"status": "error", "error": str(ar.result), "updated_at": _now_ts()})
			with _TASKS_LOCK:
				_TASKS[task_id] = entry
		else:
			# pending / started
			pass
		return {"task_id": task_id, "backend": backend, "status": entry["status"], "result": entry.get("result"), "error": entry.get("error")}

	if backend == "kafka":
		consumer = _get_kafka_consumer()
		records = consumer.poll(timeout_ms=5)
		for tp, batches in records.items():
			for msg in batches:
				val = msg.value if isinstance(msg.value, dict) else {}
				if val.get("task_id") == task_id:
					entry.update({"status": "success", "result": val.get("result", val), "updated_at": _now_ts()})
					with _TASKS_LOCK:
						_TASKS[task_id] = entry
						return {"task_id": task_id, "backend": backend, "status": entry["status"], "result": entry.get("result"), "error": entry.get("error")}
		return {"task_id": task_id, "backend": backend, "status": status, "result": entry.get("result"), "error": entry.get("error")}

	# local
	return {"task_id": task_id, "backend": backend, "status": status, "result": entry.get("result"), "error": entry.get("error")}


class LocalWorker:
	"""Local (non-Ray) worker for DeepFace operations. Source of truth for logic."""

	def _bytes_to_image_array(self, image_bytes: bytes) -> np.ndarray:
		try:
			np_buf = np.frombuffer(image_bytes, dtype=np.uint8)
			img_bgr = cv2.imdecode(np_buf, cv2.IMREAD_COLOR)
			if img_bgr is None:
				image = Image.open(io.BytesIO(image_bytes))
				if image.mode != 'RGB':
					image = image.convert('RGB')
				img_rgb = np.array(image)
				img_bgr = cv2.cvtColor(img_rgb, cv2.COLOR_RGB2BGR)
			# ensure valid, contiguous uint8 BGR image
			if not isinstance(img_bgr, np.ndarray) or img_bgr.ndim != 3 or img_bgr.shape[2] != 3:
				raise ValueError("Decoded image is not a valid BGR image")
			img_bgr = np.ascontiguousarray(img_bgr, dtype=np.uint8)
			return img_bgr
		except Exception as e:
			logger.error(f"Error converting bytes to image: {e}")
			raise ValueError(f"Invalid image format: {e}")

	def _cosine_distance(self, v1: List[float], v2: List[float]) -> float:
		import math
		dot = sum(a * b for a, b in zip(v1, v2))
		norm1 = math.sqrt(sum(a * a for a in v1))
		norm2 = math.sqrt(sum(b * b for b in v2))
		if norm1 == 0 or norm2 == 0:
			return 1.0
		return 1.0 - (dot / (norm1 * norm2))

	def _euclidean_distance(self, v1: List[float], v2: List[float], l2_normalize: bool = False) -> float:
		import math
		if l2_normalize:
			def l2norm(v):
				norm = math.sqrt(sum(a * a for a in v))
				return [a / norm for a in v] if norm > 0 else v
			v1 = l2norm(v1)
			v2 = l2norm(v2)
		return math.sqrt(sum((a - b) * (a - b) for a, b in zip(v1, v2)))

	def _default_threshold(self, model_name: str, distance_metric: str) -> float:
		# Conservative defaults compatible across common models
		name = (model_name or "").lower()
		metric = (distance_metric or "").lower()
		if metric == "cosine":
			return 0.40 if "sface" in name or "arcface" in name else 0.68
		if metric == "euclidean_l2":
			return 1.10
		if metric == "euclidean":
			return 1.30
		return 0.68

	def _verify_with_variants(self, img1_bgr: np.ndarray, img2_bgr: np.ndarray, img1_bytes: bytes, img2_bytes: bytes, *, model_name: str, detector_backend: str, distance_metric: str, enforce_detection: bool, align: bool, normalization: str) -> Dict[str, Any]:
		# Attempt 1: BGR ndarray (preferred)
		try:
			return DeepFace.verify(
				img1_path=img1_bgr,
				img2_path=img2_bgr,
				model_name=model_name,
				detector_backend=detector_backend,
				distance_metric=distance_metric,
				enforce_detection=enforce_detection,
				align=align,
				normalization=normalization,
			)
		except (AttributeError, TypeError, ValueError) as e1:
			logger.debug(f"DeepFace.verify with BGR ndarray failed: {e1}")
			# Attempt 2: RGB ndarray
			try:
				img1_rgb = cv2.cvtColor(img1_bgr, cv2.COLOR_BGR2RGB)
				img2_rgb = cv2.cvtColor(img2_bgr, cv2.COLOR_BGR2RGB)
				return DeepFace.verify(
					img1_path=img1_rgb,
					img2_path=img2_rgb,
					model_name=model_name,
					detector_backend=detector_backend,
					distance_metric=distance_metric,
					enforce_detection=enforce_detection,
					align=align,
					normalization=normalization,
				)
			except (AttributeError, TypeError, ValueError) as e2:
				logger.debug(f"DeepFace.verify with RGB ndarray failed: {e2}")
				# Attempt 3: base64 data URI (in-memory, no disk)
				try:
					b64_1 = "data:image/jpeg;base64," + base64.b64encode(img1_bytes).decode("utf-8")
					b64_2 = "data:image/jpeg;base64," + base64.b64encode(img2_bytes).decode("utf-8")
					return DeepFace.verify(
						img1_path=b64_1,
						img2_path=b64_2,
						model_name=model_name,
						detector_backend=detector_backend,
						distance_metric=distance_metric,
						enforce_detection=enforce_detection,
						align=align,
						normalization=normalization,
					)
				except (AttributeError, TypeError, ValueError) as e3:
					logger.warning(f"DeepFace.verify failed across variants; falling back to manual embedding compare: {e3}")
					# Final fallback: manual embeddings via represent to bypass verify path
					emb1 = self._represent_with_variants(img1_bgr, img1_bytes, model_name=model_name, detector_backend=detector_backend, enforce_detection=enforce_detection, align=align, normalization=normalization)
					emb2 = self._represent_with_variants(img2_bgr, img2_bytes, model_name=model_name, detector_backend=detector_backend, enforce_detection=enforce_detection, align=align, normalization=normalization)
					# Normalize structure
					vec1 = emb1[0]["embedding"] if isinstance(emb1, list) else emb1["embedding"]
					vec2 = emb2[0]["embedding"] if isinstance(emb2, list) else emb2["embedding"]
					metric = (distance_metric or "cosine").lower()
					if metric == "cosine":
						dist = self._cosine_distance(vec1, vec2)
					elif metric == "euclidean_l2":
						dist = self._euclidean_distance(vec1, vec2, l2_normalize=True)
					else:
						dist = self._euclidean_distance(vec1, vec2, l2_normalize=False)
					threshold = self._default_threshold(model_name, metric)
					return {
						"verified": dist <= threshold,
						"distance": dist,
						"threshold": threshold,
						"model": model_name,
						"detector_backend": detector_backend,
						"similarity_metric": metric,
						"facial_areas": {},
						"time": {},
					}

	def _analyze_with_variants(self, img_bgr: np.ndarray, img_bytes: bytes, *, actions: List[str], model_name: str, detector_backend: str, enforce_detection: bool, align: bool, silent: bool) -> Any:
		try:
			return DeepFace.analyze(
				img_path=img_bgr,
				actions=actions,
				model_name=model_name,
				detector_backend=detector_backend,
				enforce_detection=enforce_detection,
				align=align,
				silent=silent,
			)
		except (AttributeError, TypeError, ValueError) as e1:
			logger.debug(f"DeepFace.analyze with BGR ndarray failed: {e1}")
			try:
				img_rgb = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB)
				return DeepFace.analyze(
					img_path=img_rgb,
					actions=actions,
					model_name=model_name,
					detector_backend=detector_backend,
					enforce_detection=enforce_detection,
					align=align,
					silent=silent,
				)
			except (AttributeError, TypeError, ValueError) as e2:
				logger.debug(f"DeepFace.analyze with RGB ndarray failed: {e2}")
				b64 = "data:image/jpeg;base64," + base64.b64encode(img_bytes).decode("utf-8")
				return DeepFace.analyze(
					img_path=b64,
					actions=actions,
					model_name=model_name,
					detector_backend=detector_backend,
					enforce_detection=enforce_detection,
					align=align,
					silent=silent,
				)

	def _represent_with_variants(self, img_bgr: np.ndarray, img_bytes: bytes, *, model_name: str, detector_backend: str, enforce_detection: bool, align: bool, normalization: str) -> Any:
		try:
			return DeepFace.represent(
				img_path=img_bgr,
				model_name=model_name,
				detector_backend=detector_backend,
				enforce_detection=enforce_detection,
				align=align,
				normalization=normalization,
			)
		except (AttributeError, TypeError, ValueError) as e1:
			logger.debug(f"DeepFace.represent with BGR ndarray failed: {e1}")
			try:
				img_rgb = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB)
				return DeepFace.represent(
					img_path=img_rgb,
					model_name=model_name,
					detector_backend=detector_backend,
					enforce_detection=enforce_detection,
					align=align,
					normalization=normalization,
				)
			except (AttributeError, TypeError, ValueError) as e2:
				logger.debug(f"DeepFace.represent with RGB ndarray failed: {e2}")
				b64 = "data:image/jpeg;base64," + base64.b64encode(img_bytes).decode("utf-8")
				return DeepFace.represent(
					img_path=b64,
					model_name=model_name,
					detector_backend=detector_backend,
					enforce_detection=enforce_detection,
					align=align,
					normalization=normalization,
				)

	def verify_faces(self, job: Optional[VerifyJob] = None, **kwargs) -> Dict[str, Any]:
		if job is None:
			img1_bytes = kwargs["img1_bytes"]
			img2_bytes = kwargs["img2_bytes"]
			model_name = kwargs["model_name"]
			detector_backend = kwargs["detector_backend"]
			distance_metric = kwargs["distance_metric"]
			enforce_detection = bool(kwargs.get("enforce_detection", True))
			align = bool(kwargs.get("align", True))
			normalization = kwargs.get("normalization", "base")
			img1_array = self._bytes_to_image_array(img1_bytes)
			img2_array = self._bytes_to_image_array(img2_bytes)
			return self._verify_with_variants(
				img1_array,
				img2_array,
				img1_bytes,
				img2_bytes,
				model_name=model_name,
				detector_backend=detector_backend,
				distance_metric=distance_metric,
				enforce_detection=enforce_detection,
				align=align,
				normalization=normalization,
			)
		# job path
		img1_array = self._bytes_to_image_array(job.img1_bytes)
		img2_array = self._bytes_to_image_array(job.img2_bytes)
		return self._verify_with_variants(
			img1_array,
			img2_array,
			job.img1_bytes,
			job.img2_bytes,
			model_name=str(job.options.model_name.value),
			detector_backend=str(job.options.detector_backend.value),
			distance_metric=str(job.options.distance_metric.value),
			enforce_detection=job.options.enforce_detection,
			align=job.options.align,
			normalization=str(job.options.normalization.value),
		)

	def analyze_face(self, job: Optional[AnalyzeJob] = None, **kwargs) -> List[Dict[str, Any]]:
		if job is None:
			img_bytes = kwargs["img_bytes"]
			actions = kwargs["actions"]
			model_name = kwargs["model_name"]
			detector_backend = kwargs["detector_backend"]
			enforce_detection = bool(kwargs.get("enforce_detection", True))
			align = bool(kwargs.get("align", True))
			silent = bool(kwargs.get("silent", False))
			img_array = self._bytes_to_image_array(img_bytes)
			result = self._analyze_with_variants(
				img_array,
				img_bytes,
				actions=actions,
				model_name=model_name,
				detector_backend=detector_backend,
				enforce_detection=enforce_detection,
				align=align,
				silent=silent,
			)
			return result if isinstance(result, list) else [result]
		# job path
		img_array = self._bytes_to_image_array(job.img_bytes)
		result = self._analyze_with_variants(
			img_array,
			job.img_bytes,
			actions=[a.value for a in job.options.actions],
			model_name=str(job.options.model_name.value),
			detector_backend=str(job.options.detector_backend.value),
			enforce_detection=job.options.enforce_detection,
			align=job.options.align,
			silent=job.options.silent,
		)
		return result if isinstance(result, list) else [result]

	def find_faces(self, job: Optional[FindJob] = None, **kwargs) -> List[Dict[str, Any]]:
		if job is None:
			img_bytes = kwargs["img_bytes"]
			db_images = kwargs["db_images"]
			model_name = kwargs["model_name"]
			detector_backend = kwargs["detector_backend"]
			distance_metric = kwargs["distance_metric"]
			enforce_detection = bool(kwargs.get("enforce_detection", True))
			align = bool(kwargs.get("align", True))
			normalization = kwargs.get("normalization", "base")
			silent = bool(kwargs.get("silent", False))
			query_array = self._bytes_to_image_array(img_bytes)
			results: List[Dict[str, Any]] = []
			for db_item in db_images:
				db_img_array = self._bytes_to_image_array(db_item["image_bytes"])
				verification_result = self._verify_with_variants(
					query_array,
					db_img_array,
					img_bytes,
					db_item["image_bytes"],
					model_name=model_name,
					detector_backend=detector_backend,
					distance_metric=distance_metric,
					enforce_detection=enforce_detection,
					align=align,
					normalization=normalization,
				)
				results.append({
					"identity": db_item.get("id", "unknown"),
					"distance": verification_result["distance"],
					"threshold": verification_result["threshold"],
					"verified": verification_result["verified"],
				})
			results.sort(key=lambda x: x["distance"])
			return results
		# job path
		query_array = self._bytes_to_image_array(job.img_bytes)
		results: List[Dict[str, Any]] = []
		for db_item in job.db_images:
			db_img_array = self._bytes_to_image_array(db_item.image_bytes)
			verification_result = self._verify_with_variants(
				query_array,
				db_img_array,
				job.img_bytes,
				db_item.image_bytes,
				model_name=str(job.options.model_name.value),
				detector_backend=str(job.options.detector_backend.value),
				distance_metric=str(job.options.distance_metric.value),
				enforce_detection=job.options.enforce_detection,
				align=job.options.align,
				normalization=str(job.options.normalization.value),
			)
			results.append({
				"identity": db_item.id or "unknown",
				"distance": verification_result["distance"],
				"threshold": verification_result["threshold"],
				"verified": verification_result["verified"],
			})
		results.sort(key=lambda x: x["distance"])
		return results

	def extract_embedding(self, job: Optional[ExtractEmbeddingJob] = None, **kwargs) -> Dict[str, Any]:
		if job is None:
			img_bytes = kwargs["img_bytes"]
			model_name = kwargs["model_name"]
			detector_backend = kwargs["detector_backend"]
			enforce_detection = bool(kwargs.get("enforce_detection", True))
			align = bool(kwargs.get("align", True))
			normalization = kwargs.get("normalization", "base")
			img_array = self._bytes_to_image_array(img_bytes)
			embedding = self._represent_with_variants(
				img_array,
				img_bytes,
				model_name=model_name,
				detector_backend=detector_backend,
				enforce_detection=enforce_detection,
				align=align,
				normalization=normalization,
			)
			if isinstance(embedding, list) and embedding:
				return {"embedding": embedding[0]["embedding"], "facial_area": embedding[0]["facial_area"]}
			raise ValueError("No face detected in image")
		# job path
		img_array = self._bytes_to_image_array(job.img_bytes)
		embedding = self._represent_with_variants(
			img_array,
			job.img_bytes,
			model_name=str(job.options.model_name.value),
			detector_backend=str(job.options.detector_backend.value),
			enforce_detection=job.options.enforce_detection,
			align=job.options.align,
			normalization=str(job.options.normalization.value),
		)
		if isinstance(embedding, list) and embedding:
			return {"embedding": embedding[0]["embedding"], "facial_area": embedding[0]["facial_area"]}
		raise ValueError("No face detected in image")


@ray.remote
class RayWorker:
	"""Ray actor that delegates to LocalWorker to perform work."""

	def __init__(self):
		self.local = LocalWorker()
		logger.info("RayWorker initialized")

	def ping(self) -> str:
		return "ok"

	def verify_faces(self, *, img1_bytes: bytes, img2_bytes: bytes, model_name: str, detector_backend: str, distance_metric: str, enforce_detection: bool, align: bool, normalization: str) -> Dict[str, Any]:
		return self.local.verify_faces(
			img1_bytes=img1_bytes,
			img2_bytes=img2_bytes,
			model_name=model_name,
			detector_backend=detector_backend,
			distance_metric=distance_metric,
			enforce_detection=enforce_detection,
			align=align,
			normalization=normalization,
		)

	def analyze_face(self, *, img_bytes: bytes, actions: List[str], model_name: str, detector_backend: str, enforce_detection: bool, align: bool, silent: bool) -> List[Dict[str, Any]]:
		return self.local.analyze_face(
			img_bytes=img_bytes,
			actions=actions,
			model_name=model_name,
			detector_backend=detector_backend,
			enforce_detection=enforce_detection,
			align=align,
			silent=silent,
		)

	def find_faces(self, *, img_bytes: bytes, db_images: List[Dict[str, Any]], model_name: str, detector_backend: str, distance_metric: str, enforce_detection: bool, align: bool, normalization: str, silent: bool) -> List[Dict[str, Any]]:
		return self.local.find_faces(
			img_bytes=img_bytes,
			db_images=db_images,
			model_name=model_name,
			detector_backend=detector_backend,
			distance_metric=distance_metric,
			enforce_detection=enforce_detection,
			align=align,
			normalization=normalization,
			silent=silent,
		)

	def extract_embedding(self, *, img_bytes: bytes, model_name: str, detector_backend: str, enforce_detection: bool, align: bool, normalization: str) -> Dict[str, Any]:
		return self.local.extract_embedding(
			img_bytes=img_bytes,
			model_name=model_name,
			detector_backend=detector_backend,
			enforce_detection=enforce_detection,
			align=align,
			normalization=normalization,
		)


# Celery tasks (function-level) used when CELERY_EAGER=false
@celery_app.task(name="deepface.verify_faces")
def verify_faces_task(img1_bytes: bytes, img2_bytes: bytes, model_name: str, detector_backend: str, distance_metric: str, enforce_detection: bool, align: bool, normalization: str) -> Dict[str, Any]:
	return LocalWorker().verify_faces(
		img1_bytes=img1_bytes,
		img2_bytes=img2_bytes,
		model_name=model_name,
		detector_backend=detector_backend,
		distance_metric=distance_metric,
		enforce_detection=enforce_detection,
		align=align,
		normalization=normalization,
	)


@celery_app.task(name="deepface.analyze_face")
def analyze_face_task(img_bytes: bytes, actions: List[str], model_name: str, detector_backend: str, enforce_detection: bool, align: bool, silent: bool) -> List[Dict[str, Any]]:
	return LocalWorker().analyze_face(
		img_bytes=img_bytes,
		actions=actions,
		model_name=model_name,
		detector_backend=detector_backend,
		enforce_detection=enforce_detection,
		align=align,
		silent=silent,
	)


@celery_app.task(name="deepface.find_faces")
def find_faces_task(img_bytes: bytes, db_images: List[Dict[str, Any]], model_name: str, detector_backend: str, distance_metric: str, enforce_detection: bool, align: bool, normalization: str, silent: bool) -> List[Dict[str, Any]]:
	return LocalWorker().find_faces(
		img_bytes=img_bytes,
		db_images=db_images,
		model_name=model_name,
		detector_backend=detector_backend,
		distance_metric=distance_metric,
		enforce_detection=enforce_detection,
		align=align,
		normalization=normalization,
		silent=silent,
	)


@celery_app.task(name="deepface.extract_embedding")
def extract_embedding_task(img_bytes: bytes, model_name: str, detector_backend: str, enforce_detection: bool, align: bool, normalization: str) -> Dict[str, Any]:
	return LocalWorker().extract_embedding(
		img_bytes=img_bytes,
		model_name=model_name,
		detector_backend=detector_backend,
		enforce_detection=enforce_detection,
		align=align,
		normalization=normalization,
	)


class CeleryWorker:
	"""Celery-backed worker; uses eager local execution or submits Celery tasks when configured."""

	def verify_faces(self, *, img1_bytes: bytes, img2_bytes: bytes, model_name: str, detector_backend: str, distance_metric: str, enforce_detection: bool, align: bool, normalization: str) -> Dict[str, Any]:
		if celery_app.conf.task_always_eager:
			return LocalWorker().verify_faces(
				img1_bytes=img1_bytes,
				img2_bytes=img2_bytes,
				model_name=model_name,
				detector_backend=detector_backend,
				distance_metric=distance_metric,
				enforce_detection=enforce_detection,
				align=align,
				normalization=normalization,
			)
		async_result = verify_faces_task.delay(img1_bytes, img2_bytes, model_name, detector_backend, distance_metric, enforce_detection, align, normalization)
		return {"task_id": async_result.id}

	def analyze_face(self, *, img_bytes: bytes, actions: List[str], model_name: str, detector_backend: str, enforce_detection: bool, align: bool, silent: bool) -> Any:
		if celery_app.conf.task_always_eager:
			return LocalWorker().analyze_face(
				img_bytes=img_bytes,
				actions=actions,
				model_name=model_name,
				detector_backend=detector_backend,
				enforce_detection=enforce_detection,
				align=align,
				silent=silent,
			)
		async_result = analyze_face_task.delay(img_bytes, actions, model_name, detector_backend, enforce_detection, align, silent)
		return {"task_id": async_result.id}

	def find_faces(self, *, img_bytes: bytes, db_images: List[Dict[str, Any]], model_name: str, detector_backend: str, distance_metric: str, enforce_detection: bool, align: bool, normalization: str, silent: bool) -> Any:
		if celery_app.conf.task_always_eager:
			return LocalWorker().find_faces(
				img_bytes=img_bytes,
				db_images=db_images,
				model_name=model_name,
				detector_backend=detector_backend,
				distance_metric=distance_metric,
				enforce_detection=enforce_detection,
				align=align,
				normalization=normalization,
				silent=silent,
			)
		async_result = find_faces_task.delay(img_bytes, db_images, model_name, detector_backend, distance_metric, enforce_detection, align, normalization, silent)
		return {"task_id": async_result.id}

	def extract_embedding(self, *, img_bytes: bytes, model_name: str, detector_backend: str, enforce_detection: bool, align: bool, normalization: str) -> Any:
		if celery_app.conf.task_always_eager:
			return LocalWorker().extract_embedding(
				img_bytes=img_bytes,
				model_name=model_name,
				detector_backend=detector_backend,
				enforce_detection=enforce_detection,
				align=align,
				normalization=normalization,
			)
		async_result = extract_embedding_task.delay(img_bytes, model_name, detector_backend, enforce_detection, align, normalization)
		return {"task_id": async_result.id}


class KafkaWorker:
	"""Kafka-backed worker that publishes requests to Kafka topics and relies on get_task_status to poll results."""

	def __init__(self):
		if not _KAFKA_AVAILABLE:
			raise RuntimeError("Kafka backend requested but kafka-python is not available")
		self.producer = _get_kafka_producer()
		self.request_topic = os.getenv("KAFKA_REQUEST_TOPIC", "deepface.requests")

	def is_ready(self) -> bool:
		"""Best-effort readiness check: verifies producer is bootstrapped to a broker."""
		try:
			bootstrap_connected = getattr(self.producer, "bootstrap_connected", None)
			if callable(bootstrap_connected):
				return bool(bootstrap_connected())
			return True
		except Exception as e:
			logger.warning(f"Kafka producer not ready: {e}")
			return False

	def _send(self, kind: str, payload: Dict[str, Any]) -> Dict[str, Any]:
		correlation_id = f"kafka-{uuid.uuid4()}"
		message = {
			"task_id": correlation_id,
			"kind": kind,
			"payload": payload,
			"created_at": _now_ts(),
		}
		future = self.producer.send(self.request_topic, key=correlation_id, value=message)
		try:
			future.get(timeout=float(os.getenv("KAFKA_PRODUCE_TIMEOUT_SEC", "2")))
		except Exception as e:
			logger.warning(f"Kafka produce failed quickly: {e}")
			raise RuntimeError("Kafka produce failed")
		register_kafka_task(kind, correlation_id)
		return {"task_id": correlation_id}

	def verify_faces(self, *, img1_bytes: bytes, img2_bytes: bytes, model_name: str, detector_backend: str, distance_metric: str, enforce_detection: bool, align: bool, normalization: str) -> Dict[str, Any]:
		payload = {
			"img1": base64.b64encode(img1_bytes).decode("utf-8"),
			"img2": base64.b64encode(img2_bytes).decode("utf-8"),
			"model_name": model_name,
			"detector_backend": detector_backend,
			"distance_metric": distance_metric,
			"enforce_detection": enforce_detection,
			"align": align,
			"normalization": normalization,
		}
		return self._send("verify", payload)

	def analyze_face(self, *, img_bytes: bytes, actions: List[str], model_name: str, detector_backend: str, enforce_detection: bool, align: bool, silent: bool) -> Any:
		payload = {
			"image": base64.b64encode(img_bytes).decode("utf-8"),
			"actions": actions,
			"model_name": model_name,
			"detector_backend": detector_backend,
			"enforce_detection": enforce_detection,
			"align": align,
			"silent": silent,
		}
		return self._send("analyze", payload)

	def find_faces(self, *, img_bytes: bytes, db_images: List[Dict[str, Any]], model_name: str, detector_backend: str, distance_metric: str, enforce_detection: bool, align: bool, normalization: str, silent: bool) -> Any:
		encoded_db: List[Dict[str, Any]] = []
		for item in db_images:
			encoded_db.append({
				"id": item.get("id"),
				"image": base64.b64encode(item["image_bytes"]).decode("utf-8"),
			})
		payload = {
			"image": base64.b64encode(img_bytes).decode("utf-8"),
			"db": encoded_db,
			"model_name": model_name,
			"detector_backend": detector_backend,
			"distance_metric": distance_metric,
			"enforce_detection": enforce_detection,
			"align": align,
			"normalization": normalization,
			"silent": silent,
		}
		return self._send("find", payload)

	def extract_embedding(self, *, img_bytes: bytes, model_name: str, detector_backend: str, enforce_detection: bool, align: bool, normalization: str) -> Any:
		payload = {
			"image": base64.b64encode(img_bytes).decode("utf-8"),
			"model_name": model_name,
			"detector_backend": detector_backend,
			"enforce_detection": enforce_detection,
			"align": align,
			"normalization": normalization,
		}
		return self._send("embed", payload)


@ray.remote
def get_deepface_models() -> Dict[str, List[str]]:
	try:
		models = [
			"VGG-Face", "Facenet", "Facenet512", "OpenFace", 
			"DeepFace", "DeepID", "ArcFace", "Dlib", "SFace", "GhostFaceNet"
		]
		detectors = [
			"opencv", "ssd", "dlib", "mtcnn", 
			"retinaface", "mediapipe", "yolov8", "yunet"
		]
		return {
			"models": models,
			"detectors": detectors,
			"distance_metrics": ["cosine", "euclidean", "euclidean_l2"]
		}
	except Exception as e:
		logger.error(f"Error getting models: {e}")
		raise RuntimeError(f"Failed to get models: {e}")


def get_deepface_models_local() -> Dict[str, List[str]]:
	models = [
		"VGG-Face", "Facenet", "Facenet512", "OpenFace", 
		"DeepFace", "DeepID", "ArcFace", "Dlib", "SFace", "GhostFaceNet"
	]
	detectors = [
		"opencv", "ssd", "dlib", "mtcnn", 
		"retinaface", "mediapipe", "yolov8", "yunet"
	]
	return {
		"models": models,
		"detectors": detectors,
		"distance_metrics": ["cosine", "euclidean", "euclidean_l2"]
	} 