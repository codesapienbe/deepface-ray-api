from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from enum import Enum

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

class VerifyRequest(BaseModel):
	model_name: ModelName = ModelName.VGG_FACE
	detector_backend: DetectorBackend = DetectorBackend.OPENCV
	distance_metric: DistanceMetric = DistanceMetric.COSINE
	enforce_detection: bool = True
	align: bool = True
	normalization: Normalization = Normalization.BASE

class AnalyzeRequest(BaseModel):
	actions: List[Action] = Field(default=[Action.AGE, Action.GENDER, Action.EMOTION, Action.RACE])
	model_name: ModelName = ModelName.VGG_FACE
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

class FindRequest(BaseModel):
	model_name: ModelName = ModelName.VGG_FACE
	detector_backend: DetectorBackend = DetectorBackend.OPENCV
	distance_metric: DistanceMetric = DistanceMetric.COSINE
	enforce_detection: bool = True
	align: bool = True
	normalization: Normalization = Normalization.BASE
	silent: bool = False

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
	model_name: ModelName = ModelName.VGG_FACE
	detector_backend: DetectorBackend = DetectorBackend.OPENCV
	enforce_detection: bool = True
	align: bool = True
	normalization: Normalization = Normalization.BASE

class ExtractEmbeddingResponse(BaseModel):
	embedding: List[float]
	facial_area: Dict[str, Any]

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
