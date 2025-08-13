# DeepFace Ray API Makefile

IMAGE_NAME ?= deepface-ray-api
IMAGE_TAG ?= latest
REGISTRY ?=

.PHONY: clean verify install start stop deploy


verify:
	docker run --rm -v $(CURDIR):/app -w /app ghcr.io/astral-sh/ruff:latest ruff check app/
	docker run --rm -v $(CURDIR):/app -w /app ghcr.io/astral-sh/ruff:latest ruff format --check app/

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .pytest_cache
	rm -rf /tmp/ray
	docker rm -f $(IMAGE_NAME) || true

install:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

start:
	docker rm -f $(IMAGE_NAME) || true
	docker run -d \
		--name $(IMAGE_NAME) \
		-p 8000:8000 \
		-e RAY_object_store_memory=1073741824 \
		-e RAY_spill_dir=/tmp/ray/spill \
		-e RAY_enable_object_reconstruction=1 \
		-e NUM_WORKERS=1 \
		-e RAY_ADDRESS=auto \
		-e MAX_IMAGE_SIZE=1024 \
		-v /tmp/ray:/tmp/ray \
		--shm-size=4g \
		$(IMAGE_NAME):$(IMAGE_TAG)


stop:
	docker stop $(IMAGE_NAME) || true
	docker rm -f $(IMAGE_NAME) || true


deploy:
	@test -n "$(REGISTRY)" || { echo "REGISTRY is required for deploy (e.g., REGISTRY=registry.example.com)"; exit 1; }
	docker tag $(IMAGE_NAME):$(IMAGE_TAG) $(REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)
	docker push $(REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)

