# DeepFace Ray API Makefile

IMAGE_NAME ?= deepface-ray-api
IMAGE_TAG ?= latest
REGISTRY ?=

.PHONY: help install run test deploy clean verify deps serve logs stop

help:
	@echo "Available commands:"
	@echo "  install      - Build Docker image ($(IMAGE_NAME):$(IMAGE_TAG))"
	@echo "  run          - Run the application via docker compose"
	@echo "  deploy       - Tag and push image to registry (requires REGISTRY env)"
	@echo "  test         - Run the test client inside a container"
	@echo "  verify       - Lint and format checks with ruff (containerized)"
	@echo "  logs         - Tail application logs"
	@echo "  stop         - Stop containers"
	@echo "  clean        - Clean up temporary files and stop containers"

verify:
	docker run --rm -v $(CURDIR):/app -w /app ghcr.io/astral-sh/ruff:latest ruff check app/
	docker run --rm -v $(CURDIR):/app -w /app ghcr.io/astral-sh/ruff:latest ruff format --check app/

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .pytest_cache
	rm -rf /tmp/ray
	docker compose down -v || true
	docker rm -f deepface-ray-api || true
	docker rm -f ray-head || true

install:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

run:
	docker compose up -d

logs:
	docker compose logs -f

stop:
	docker compose down

test:
	docker run --rm --network host $(IMAGE_NAME):$(IMAGE_TAG) uv run python test_client.py

# In-container targets used by Dockerfile

deps:
	uv pip install --system --no-cache-dir -r requirements.txt

serve:
	uv run python run.py

deploy:
	@test -n "$(REGISTRY)" || { echo "REGISTRY is required for deploy (e.g., REGISTRY=registry.example.com)"; exit 1; }
	docker tag $(IMAGE_NAME):$(IMAGE_TAG) $(REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)
	docker push $(REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)

