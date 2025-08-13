from pydantic import BaseModel, Field
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

class VerifyRequest(BaseModel):
    model_name: ModelName = ModelName.VGG_FACE
    detector_backend: DetectorBackend = DetectorBackend.OPENCV
    distance_metric: DistanceMetric = DistanceMetric.COSINE
    enforce_detection: bool = True
    align: bool = True
    normalization: str = "base"

class AnalyzeRequest(BaseModel):
    actions: List[str] = Field(default=["age", "gender", "emotion", "race"])
    model_name: ModelName = ModelName.VGG_FACE
    detector_backend: DetectorBackend = DetectorBackend.OPENCV
    enforce_detection: bool = True
    align: bool = True
    silent: bool = False

class FindRequest(BaseModel):
    model_name: ModelName = ModelName.VGG_FACE
    detector_backend: DetectorBackend = DetectorBackend.OPENCV
    distance_metric: DistanceMetric = DistanceMetric.COSINE
    enforce_detection: bool = True
    align: bool = True
    normalization: str = "base"
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
    normalization: str = "base"

class ExtractEmbeddingResponse(BaseModel):
    embedding: List[float]
    facial_area: Dict[str, Any]
