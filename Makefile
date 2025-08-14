# DeepFace Ray API Makefile

IMAGE_NAME ?= deepface-ray-api
IMAGE_TAG ?= latest
REGISTRY ?=
KAFKA_BOOTSTRAP_SERVERS ?= host.docker.internal:9092
KAFKA_IMAGE ?= bitnami/kafka:latest
KAFKA_CONTAINER_NAME ?= kafka

.PHONY: clean verify install start stop deploy restart


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
	# Single-node Kafka (KRaft) with external access via host.docker.internal:9092
	docker rm -f $(KAFKA_CONTAINER_NAME) || true
	docker run -d \
		--name $(KAFKA_CONTAINER_NAME) \
		-p 9092:9092 \
		-e KAFKA_CFG_NODE_ID=0 \
		-e KAFKA_CFG_PROCESS_ROLES=broker,controller \
		-e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@127.0.0.1:9093 \
		-e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
		-e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://host.docker.internal:9092 \
		-e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
		-e KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
		-e ALLOW_PLAINTEXT_LISTENER=yes \
		$(KAFKA_IMAGE)
	# Start API (DeepFace Ray API) 
	docker rm -f $(IMAGE_NAME) || true
	docker run -d \
		--name $(IMAGE_NAME) \
		-v ~/.deepface:/root/.deepface \
		-p 8000:8000 \
		-e RAY_object_store_memory=1073741824 \
		-e RAY_spill_dir=/tmp/ray/spill \
		-e RAY_enable_object_reconstruction=1 \
		-e NUM_WORKERS=1 \
		-e RAY_ADDRESS=auto \
		-e MAX_IMAGE_SIZE=1024 \
		-e WORKER_PROVIDER=kafka \
		-e KAFKA_BOOTSTRAP_SERVERS=$(KAFKA_BOOTSTRAP_SERVERS) \
		-v /tmp/ray:/tmp/ray \
		--shm-size=4g \
		$(IMAGE_NAME):$(IMAGE_TAG)

logs:
	@echo "Attaching to logs for Kafka container: $(KAFKA_CONTAINER_NAME)"
	docker logs -f $(KAFKA_CONTAINER_NAME) &
	@echo "Attaching to logs for API container: $(IMAGE_NAME)"
	docker logs -f $(IMAGE_NAME)

stop:
	docker stop $(IMAGE_NAME) || true
	docker rm -f $(IMAGE_NAME) || true
	docker stop $(KAFKA_CONTAINER_NAME) || true
	docker rm -f $(KAFKA_CONTAINER_NAME) || true

restart:
	$(MAKE) stop
	$(MAKE) start

deploy:
	@test -n "$(REGISTRY)" || { echo "REGISTRY is required for deploy (e.g., REGISTRY=registry.example.com)"; exit 1; }
	docker tag $(IMAGE_NAME):$(IMAGE_TAG) $(REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)
	docker push $(REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)

