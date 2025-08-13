# DeepFace Ray API

A scalable FastAPI application that provides a distributed API for DeepFace facial recognition and analysis using Ray for distributed computing.

## Features

- **Face Verification**: Compare two faces to verify if they belong to the same person
- **Face Analysis**: Analyze facial attributes (age, gender, emotion, race)
- **Face Search**: Find similar faces in a database of images
- **Embedding Extraction**: Extract face embeddings/representations
- **Batch Processing**: Process multiple images simultaneously
- **Stream-based**: No local file I/O, all operations use image streams
- **Scalable**: Uses Ray for distributed computing across multiple workers

## Quick Start

### Using Docker (Recommended)

```bash
# Build the Docker image
docker build -t deepface-ray-api .

# Run the container
docker run -p 8000:8000 deepface-ray-api
```

### Using Docker Compose

- Default single-container mode (Ray local mode):

```bash
docker-compose up -d
```

- Multi-node mode with a dedicated Ray head (enable the optional profile):

```bash
# Launch API + Ray head node
docker-compose --profile multi-node up -d

# Or, if you already have the stack up, start ray-head separately:
docker-compose --profile multi-node up -d ray-head

# Point the API to the head explicitly if overriding defaults
# (compose already defaults RAY_ADDRESS to auto)
RAY_ADDRESS=ray-head:6379 docker-compose --profile multi-node up -d deepface-api
```

### Manual Installation

```bash
# Install uv (if not installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install project dependencies (from pyproject.toml)
uv sync

# Start Ray (optional, for distributed computing)
ray start --head --port=6379

# Run the application
uv run python -m app.main
```

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

- `RAY_ADDRESS`: Ray cluster address (default: "auto"). When using the optional `ray-head` service via compose profile, set `RAY_ADDRESS=ray-head:6379`.
- `NUM_WORKERS`: Number of DeepFace workers (default: 4)
- `MAX_IMAGE_SIZE`: Maximum image size for processing (default: 1024)

### Request Parameters

All endpoints support various model configurations:

- **Models**: VGG-Face, Facenet, Facenet512, OpenFace, DeepFace, DeepID, ArcFace, Dlib, SFace, GhostFaceNet
- **Detectors**: opencv, ssd, dlib, mtcnn, retinaface, mediapipe, yolov8, yunet
- **Distance Metrics**: cosine, euclidean, euclidean_l2

## Scaling

### Vertical Scaling

Increase the number of workers:

```python
num_workers = 8  # Increase in app/main.py
```

### Horizontal Scaling

Add more machines to the Ray cluster:

```bash
# On additional machines
ray start --address='<head-node-ip>:6379'
```

### Auto-scaling

Ray can automatically scale based on workload:

```bash
ray up cluster-config.yaml
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

1. **Ray not starting**: Ensure no other Ray processes are running

   ```bash
   ray stop
   ray start --head --port=6379
   ```

2. **Memory issues**: Reduce the number of workers or image size

   ```python
   num_workers = 2  # Reduce workers
   MAX_IMAGE_SIZE = 512  # Reduce image size
   ```

3. **Model download errors**: Ensure internet connectivity for first-time model downloads

### Logs

Check application logs for detailed error information:

```bash
docker logs <container-id>
```

## Development

### Project Structure

```
deepface-ray-api/
├── app/
│   ├── __init__.py          # Package initialization
│   ├── main.py              # FastAPI application
│   ├── models.py            # Pydantic models
│   ├── ray_tasks.py         # Ray remote functions
│   └── utils.py             # Utility functions
├── pyproject.toml           # Project metadata and dependencies
├── Dockerfile              # Container configuration
└── README.md               # This file
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
