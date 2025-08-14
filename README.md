# DeepFace Ray API

A scalable FastAPI application that provides a distributed API for DeepFace facial recognition and analysis. Default worker backend is Kafka, with support for Ray, Celery, and Local backends.

## Features

- **Face Verification**: Compare two faces to verify if they belong to the same person
- **Face Analysis**: Analyze facial attributes (age, gender, emotion, race)
- **Face Search**: Find similar faces in a database of images
- **Embedding Extraction**: Extract face embeddings/representations
- **Batch Processing**: Process multiple images simultaneously
- **Stream-based**: No local file I/O, all operations use image streams
- **Scalable**: Uses Ray for distributed computing across multiple workers

## Quick Start

### Using Makefile (Recommended)

```bash
# Build the Docker image
make install

# Start the container
make start

# Stop the container
make stop

# Lint and format checks
make verify

# Clean artifacts and stop containers
make clean
```

### Environment

- You can configure runtime via environment variables passed to `make start` by overriding in the shell, for example:

```bash
# Example: override workers and max image size
NUM_WORKERS=2 MAX_IMAGE_SIZE=1024 make start
```

- Key variables:
  - `RAY_ADDRESS` (default: auto)
  - `NUM_WORKERS` (default: 1)
  - `MAX_IMAGE_SIZE` (default: 1024)


### Manual Installation (Optional)

Containerized workflow via Makefile is recommended. If you still want to run locally, refer to the Dockerfile for required system packages and ensure Python 3.10 with all dependencies from `pyproject.toml` are installed.

## API Documentation

Once the application is running, visit:

- **Swagger UI**: <http://localhost:8000/docs>
- **ReDoc**: <http://localhost:8000/redoc>

## API Endpoints

### Health Check

```bash
curl http://localhost:8000/health
```

### Get Available Models

```bash
curl http://localhost:8000/models
```

### Face Verification

Compare two face images:

```bash
curl -X POST "http://localhost:8000/verify" \
  -F "img1=@face1.jpg" \
  -F "img2=@face2.jpg"
```

### Face Analysis

Analyze facial attributes:

```bash
curl -X POST "http://localhost:8000/analyze" \
  -F "image=@face.jpg"
```

### Face Search

Find similar faces in a database:

```bash
curl -X POST "http://localhost:8000/find" \
  -F "query_image=@query.jpg" \
  -F "database_images=@db1.jpg" \
  -F "database_images=@db2.jpg"
```

### Extract Face Embedding

```bash
curl -X POST "http://localhost:8000/extract-embedding" \
  -F "image=@face.jpg"
```

### Batch Processing

Analyze multiple images:

```bash
curl -X POST "http://localhost:8000/batch-analyze" \
  -F "images=@face1.jpg" \
  -F "images=@face2.jpg" \
  -F "images=@face3.jpg"
```

## Configuration

### Environment Variables

- `WORKER_PROVIDER`: Worker backend selection (default: "kafka"). Options: `kafka`, `ray`, `celery`, `local`. If `kafka` is selected but brokers are unreachable, the app automatically falls back to in-process Celery; if Celery is disabled, it falls back to Local.
- `KAFKA_BOOTSTRAP_SERVERS`: Comma-separated broker list (default: `localhost:9092`).
- `KAFKA_REQUEST_TOPIC`: Topic for requests (default: `deepface.requests`).
- `KAFKA_RESPONSE_TOPIC`: Topic for responses (default: `deepface.responses`).
- `KAFKA_GROUP_ID`: Consumer group id for response polling (default: `deepface-api`).
- `KAFKA_AUTO_OFFSET_RESET`: Consumer start policy (default: `earliest`).
- `RAY_ADDRESS`: Ray cluster address (default: "auto"). For external clusters, set e.g. `RAY_ADDRESS=<head-ip>:6379`.
- `NUM_WORKERS`: Number of DeepFace workers (default: 1)
- `MAX_IMAGE_SIZE`: Maximum image size for processing (default: 1024)

### Request Parameters

All endpoints support various model configurations:

- **Models**: VGG-Face, Facenet, Facenet512, OpenFace, DeepFace, DeepID, ArcFace, Dlib, SFace, GhostFaceNet
- **Detectors**: opencv, ssd, dlib, mtcnn, retinaface, mediapipe, yolov8, yunet
- **Distance Metrics**: cosine, euclidean, euclidean_l2

## Scaling

### Vertical Scaling

Increase the number of workers:

```bash
NUM_WORKERS=8 make start
```

### Horizontal Scaling

Connect to an external Ray head (advanced):

```bash
RAY_ADDRESS=<head-node-ip>:6379 make start
```

## Python Client Example

```python
import requests
import json

# Face verification
with open('face1.jpg', 'rb') as f1, open('face2.jpg', 'rb') as f2:
    response = requests.post(
        'http://localhost:8000/verify',
        files={'img1': f1, 'img2': f2}
    )
    result = response.json()
    print(f"Verified: {result['verified']}")

# Face analysis
with open('face.jpg', 'rb') as f:
    response = requests.post(
        'http://localhost:8000/analyze',
        files={'image': f}
    )
    result = response.json()
    print(f"Analysis: {result['results']}")
```

## Performance

The Ray-based architecture provides significant performance improvements:

- **Parallel Processing**: Multiple faces can be processed simultaneously
- **Resource Management**: Automatic GPU/CPU resource allocation
- **Fault Tolerance**: Automatic recovery from worker failures
- **Load Balancing**: Requests are distributed across available workers

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   FastAPI       │    │   Ray Cluster   │    │   DeepFace      │
│   Web Server    │───▶│   Scheduler     │───▶│   Workers       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
        │                       │                       │
        ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   HTTP Requests │    │   Task Queue    │    │   GPU/CPU       │
│   (Image Streams)│    │   Distribution  │    │   Processing    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Troubleshooting

### Common Issues

1. **Ray not starting**: Use the container lifecycle commands

```bash
make stop
make start
```

2. **Memory issues**: Reduce the number of workers or image size

```bash
NUM_WORKERS=2 MAX_IMAGE_SIZE=512 make start
```

3. **Model download errors**: Ensure internet connectivity for first-time model downloads

### Logs

Check application logs for detailed error information:

```bash
# Follow logs of the running container by name
docker logs -f deepface-ray-api
```

## Development

### Project Structure

```
deepface-ray-api/
├── app/
│   ├── __init__.py
│   ├── main.py
│   ├── models.py
│   ├── tasks.py
│   └── utils.py
├── pyproject.toml
├── Dockerfile
├── Makefile
└── README.md
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License.

## Support

For support and questions:

- Create an issue on GitHub
- Check the documentation at `/docs`
- Review Ray documentation for distributed computing questions
