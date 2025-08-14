from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from enum import Enum
from fastapi import Form

class ModelName(str, Enum):
	VGG_FACE = "VGG-Face"
	FACENET = "Facenet"
	FACENET512 = "Facenet512"
	OPENFACE = "OpenFace"
	DEEPFACE = "DeepFace"
	DEEPID = "DeepID"
	ARCFACE = "ArcFace"
	DLIB = "Dlib"
	SFACE = "SFace"
	GHOSTFACENET = "GhostFaceNet"

class DetectorBackend(str, Enum):
	OPENCV = "opencv"
	SSD = "ssd"
	DLIB = "dlib"
	MTCNN = "mtcnn"
	RETINAFACE = "retinaface"
	MEDIAPIPE = "mediapipe"
	YOLOV8 = "yolov8"
	YUNET = "yunet"

class DistanceMetric(str, Enum):
	COSINE = "cosine"
	EUCLIDEAN = "euclidean"
	EUCLIDEAN_L2 = "euclidean_l2"

class Action(str, Enum):
	AGE = "age"
	GENDER = "gender"
	EMOTION = "emotion"
	RACE = "race"

class Normalization(str, Enum):
	BASE = "base"
	RAW = "raw"
	FACENET = "Facenet"
	VGGFACE2 = "VGGFace2"
	ARCFACE = "ArcFace"

class BackendProvider(str, Enum):
	RAY = "ray"
	CELERY = "celery"
	LOCAL = "local"
	KAFKA = "kafka"

class TaskStatus(str, Enum):
	PENDING = "pending"
	SUCCESS = "success"
	ERROR = "error"
	NOT_FOUND = "not_found"

class VerifyRequest(BaseModel):
	model_name: ModelName = ModelName.SFACE
	detector_backend: DetectorBackend = DetectorBackend.OPENCV
	distance_metric: DistanceMetric = DistanceMetric.COSINE
	enforce_detection: bool = True
	align: bool = True
	normalization: Normalization = Normalization.BASE

	@classmethod
	def as_form(
		cls,
		model_name: ModelName = Form(ModelName.SFACE),
		detector_backend: DetectorBackend = Form(DetectorBackend.OPENCV),
		distance_metric: DistanceMetric = Form(DistanceMetric.COSINE),
		enforce_detection: bool = Form(True),
		align: bool = Form(True),
		normalization: Normalization = Form(Normalization.BASE),
	) -> "VerifyRequest":
		return cls(
			model_name=model_name,
			detector_backend=detector_backend,
			distance_metric=distance_metric,
			enforce_detection=enforce_detection,
			align=align,
			normalization=normalization,
		)

class AnalyzeRequest(BaseModel):
	actions: List[Action] = Field(default=[Action.AGE, Action.GENDER, Action.EMOTION, Action.RACE])
	model_name: ModelName = ModelName.SFACE
	detector_backend: DetectorBackend = DetectorBackend.OPENCV
	enforce_detection: bool = True
	align: bool = True
	silent: bool = False

	@validator("actions")
	def validate_actions(cls, v: List[Action]) -> List[Action]:
		if not v:
			raise ValueError("At least one action must be specified")
		# ensure uniqueness and limit length
		unique = list(dict.fromkeys(v))
		if len(unique) > 4:
			raise ValueError("A maximum of 4 actions are allowed")
		return unique

	@classmethod
	def as_form(
		cls,
		model_name: ModelName = Form(ModelName.SFACE),
		detector_backend: DetectorBackend = Form(DetectorBackend.OPENCV),
		enforce_detection: bool = Form(True),
		align: bool = Form(True),
		silent: bool = Form(False),
	) -> "AnalyzeRequest":
		# Note: actions not accepted via form for simplicity; defaults used
		return cls(
			model_name=model_name,
			detector_backend=detector_backend,
			enforce_detection=enforce_detection,
			align=align,
			silent=silent,
		)

class FindRequest(BaseModel):
	model_name: ModelName = ModelName.SFACE
	detector_backend: DetectorBackend = DetectorBackend.OPENCV
	distance_metric: DistanceMetric = DistanceMetric.COSINE
	enforce_detection: bool = True
	align: bool = True
	normalization: Normalization = Normalization.BASE
	silent: bool = False

	@classmethod
	def as_form(
		cls,
		model_name: ModelName = Form(ModelName.SFACE),
		detector_backend: DetectorBackend = Form(DetectorBackend.OPENCV),
		distance_metric: DistanceMetric = Form(DistanceMetric.COSINE),
		enforce_detection: bool = Form(True),
		align: bool = Form(True),
		normalization: Normalization = Form(Normalization.BASE),
		silent: bool = Form(False),
	) -> "FindRequest":
		return cls(
			model_name=model_name,
			detector_backend=detector_backend,
			distance_metric=distance_metric,
			enforce_detection=enforce_detection,
			align=align,
			normalization=normalization,
			silent=silent,
		)

class VerifyResponse(BaseModel):
	verified: bool
	distance: float
	threshold: float
	model: str
	detector_backend: str
	similarity_metric: str
	facial_areas: Dict[str, Any]
	time: Dict[str, float]

class AnalyzeResponse(BaseModel):
	results: List[Dict[str, Any]]

class FindResponse(BaseModel):
	results: List[Dict[str, Any]]

class ExtractEmbeddingRequest(BaseModel):
	model_name: ModelName = ModelName.SFACE
	detector_backend: DetectorBackend = DetectorBackend.OPENCV
	enforce_detection: bool = True
	align: bool = True
	normalization: Normalization = Normalization.BASE

	@classmethod
	def as_form(
		cls,
		model_name: ModelName = Form(ModelName.SFACE),
		detector_backend: DetectorBackend = Form(DetectorBackend.OPENCV),
		enforce_detection: bool = Form(True),
		align: bool = Form(True),
		normalization: Normalization = Form(Normalization.BASE),
	) -> "ExtractEmbeddingRequest":
		return cls(
			model_name=model_name,
			detector_backend=detector_backend,
			enforce_detection=enforce_detection,
			align=align,
			normalization=normalization,
		)

class ExtractEmbeddingResponse(BaseModel):
	embedding: List[float]
	facial_area: Dict[str, Any]

# Task envelope models per operation
class VerifyTaskResponse(BaseModel):
	backend: BackendProvider
	task_id: Optional[str] = None
	status: TaskStatus
	result: Optional[VerifyResponse] = None
	error: Optional[str] = None

class AnalyzeTaskResponse(BaseModel):
	backend: BackendProvider
	task_id: Optional[str] = None
	status: TaskStatus
	result: Optional[AnalyzeResponse] = None
	error: Optional[str] = None

class FindTaskResponse(BaseModel):
	backend: BackendProvider
	task_id: Optional[str] = None
	status: TaskStatus
	result: Optional[FindResponse] = None
	error: Optional[str] = None

class ExtractEmbeddingTaskResponse(BaseModel):
	backend: BackendProvider
	task_id: Optional[str] = None
	status: TaskStatus
	result: Optional[ExtractEmbeddingResponse] = None
	error: Optional[str] = None

class TaskStatusResponse(BaseModel):
	task_id: str
	backend: Optional[BackendProvider] = None
	status: TaskStatus
	kind: Optional[str] = None
	result: Optional[Dict[str, Any]] = None
	error: Optional[str] = None
	created_at: Optional[float] = None
	updated_at: Optional[float] = None

# Encrypted payload models
class EncryptedImage(BaseModel):
	nonce: str
	ciphertext: str

class SecureVerifyRequest(BaseModel):
	img1: EncryptedImage
	img2: EncryptedImage
	options: VerifyRequest = VerifyRequest()

class SecureAnalyzeRequest(BaseModel):
	image: EncryptedImage
	options: AnalyzeRequest = AnalyzeRequest()

# OOP Job request models for internal task execution
class DbImage(BaseModel):
	id: Optional[str] = None
	image_bytes: bytes

class VerifyJob(BaseModel):
	img1_bytes: bytes
	img2_bytes: bytes
	options: VerifyRequest

class AnalyzeJob(BaseModel):
	img_bytes: bytes
	options: AnalyzeRequest

class FindJob(BaseModel):
	img_bytes: bytes
	db_images: List[DbImage]
	options: FindRequest

class ExtractEmbeddingJob(BaseModel):
	img_bytes: bytes
	options: ExtractEmbeddingRequest
