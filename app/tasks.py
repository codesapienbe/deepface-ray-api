import ray
import numpy as np
from deepface import DeepFace
from PIL import Image
import io
import base64
from typing import List, Dict, Any, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@ray.remote
class DeepFaceWorker:
    """Ray actor for handling DeepFace operations with state management."""

    def __init__(self):
        """Initialize the DeepFace worker."""
        self.initialized = False
        logger.info("DeepFace worker initialized")

    def ping(self) -> str:
        """Health check ping."""
        return "ok"

    def _bytes_to_image_array(self, image_bytes: bytes) -> np.ndarray:
        """Convert bytes to numpy array for DeepFace processing."""
        try:
            image = Image.open(io.BytesIO(image_bytes))
            if image.mode != 'RGB':
                image = image.convert('RGB')
            return np.array(image)
        except Exception as e:
            logger.error(f"Error converting bytes to image: {e}")
            raise ValueError(f"Invalid image format: {e}")

    def verify_faces(
        self,
        img1_bytes: bytes,
        img2_bytes: bytes,
        model_name: str = "VGG-Face",
        detector_backend: str = "opencv",
        distance_metric: str = "cosine",
        enforce_detection: bool = True,
        align: bool = True,
        normalization: str = "base"
    ) -> Dict[str, Any]:
        """Verify if two face images belong to the same person."""
        try:
            img1_array = self._bytes_to_image_array(img1_bytes)
            img2_array = self._bytes_to_image_array(img2_bytes)

            result = DeepFace.verify(
                img1_path=img1_array,
                img2_path=img2_array,
                model_name=model_name,
                detector_backend=detector_backend,
                distance_metric=distance_metric,
                enforce_detection=enforce_detection,
                align=align,
                normalization=normalization
            )

            logger.info(f"Face verification completed: {result['verified']}")
            return result

        except Exception as e:
            logger.error(f"Error in face verification: {e}")
            raise RuntimeError(f"Face verification failed: {e}")

    def analyze_face(
        self,
        img_bytes: bytes,
        actions: List[str] = ["age", "gender", "emotion", "race"],
        model_name: str = "VGG-Face",
        detector_backend: str = "opencv",
        enforce_detection: bool = True,
        align: bool = True,
        silent: bool = False
    ) -> List[Dict[str, Any]]:
        """Analyze facial attributes like age, gender, emotion, race."""
        try:
            img_array = self._bytes_to_image_array(img_bytes)

            result = DeepFace.analyze(
                img_path=img_array,
                actions=actions,
                model_name=model_name,
                detector_backend=detector_backend,
                enforce_detection=enforce_detection,
                align=align,
                silent=silent
            )

            # Ensure result is a list
            if not isinstance(result, list):
                result = [result]

            logger.info(f"Face analysis completed for {len(result)} faces")
            return result

        except Exception as e:
            logger.error(f"Error in face analysis: {e}")
            raise RuntimeError(f"Face analysis failed: {e}")

    def find_faces(
        self,
        img_bytes: bytes,
        db_images: List[Dict[str, Any]],  # List of {"id": str, "image_bytes": bytes}
        model_name: str = "VGG-Face",
        detector_backend: str = "opencv",
        distance_metric: str = "cosine",
        enforce_detection: bool = True,
        align: bool = True,
        normalization: str = "base",
        silent: bool = False
    ) -> List[Dict[str, Any]]:
        """Find similar faces in a database of images."""
        try:
            img_array = self._bytes_to_image_array(img_bytes)

            # Create a temporary database using embeddings
            results = []

            for db_item in db_images:
                try:
                    db_img_array = self._bytes_to_image_array(db_item["image_bytes"])

                    # Verify against each database image
                    verification_result = DeepFace.verify(
                        img1_path=img_array,
                        img2_path=db_img_array,
                        model_name=model_name,
                        detector_backend=detector_backend,
                        distance_metric=distance_metric,
                        enforce_detection=enforce_detection,
                        align=align,
                        normalization=normalization
                    )

                    results.append({
                        "identity": db_item.get("id", "unknown"),
                        "distance": verification_result["distance"],
                        "threshold": verification_result["threshold"],
                        "verified": verification_result["verified"]
                    })

                except Exception as e:
                    logger.warning(f"Error processing database image {db_item.get('id', 'unknown')}: {e}")
                    continue

            # Sort by distance (most similar first)
            results.sort(key=lambda x: x["distance"])

            logger.info(f"Face search completed, found {len(results)} matches")
            return results

        except Exception as e:
            logger.error(f"Error in face search: {e}")
            raise RuntimeError(f"Face search failed: {e}")

    def extract_embedding(
        self,
        img_bytes: bytes,
        model_name: str = "VGG-Face",
        detector_backend: str = "opencv",
        enforce_detection: bool = True,
        align: bool = True,
        normalization: str = "base"
    ) -> Dict[str, Any]:
        """Extract face embedding/representation."""
        try:
            img_array = self._bytes_to_image_array(img_bytes)

            embedding = DeepFace.represent(
                img_path=img_array,
                model_name=model_name,
                detector_backend=detector_backend,
                enforce_detection=enforce_detection,
                align=align,
                normalization=normalization
            )

            # DeepFace.represent returns a list of dictionaries
            if isinstance(embedding, list) and len(embedding) > 0:
                result = {
                    "embedding": embedding[0]["embedding"],
                    "facial_area": embedding[0]["facial_area"]
                }
            else:
                raise ValueError("No face detected in image")

            logger.info("Face embedding extraction completed")
            return result

        except Exception as e:
            logger.error(f"Error in embedding extraction: {e}")
            raise RuntimeError(f"Embedding extraction failed: {e}")

@ray.remote
def get_deepface_models() -> Dict[str, List[str]]:
    """Get available models and backends."""
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