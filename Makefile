# DeepFace Ray API Makefile

.PHONY: help install run test docker-build docker-run clean

help:
	@echo "Available commands:"
	@echo "  install      - Install Python dependencies"
	@echo "  run          - Run the development server"
	@echo "  test         - Run the test client"
	@echo "  docker-build - Build Docker image"
	@echo "  docker-run   - Run Docker container"
	@echo "  docker-compose - Start with docker-compose"
	@echo "  clean        - Clean up temporary files"

install:
	pip install -r requirements.txt

run:
	python run.py

run-debug:
	python run.py --debug

run-workers-8:
	python run.py --workers 8

test:
	python test_client.py

docker-build:
	docker build -t deepface-ray-api .

docker-run: docker-build
	docker run -p 8000:8000 deepface-ray-api

docker-compose:
	docker-compose up --build

docker-compose-multi:
	docker-compose --profile multi-node up --build

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .pytest_cache
	rm -rf /tmp/ray
	docker system prune -f

# Development helpers
format:
	black app/
	isort app/

lint:
	flake8 app/
	mypy app/

# Ray cluster management
ray-start:
	ray start --head --port=6379 --dashboard-host=0.0.0.0

ray-stop:
	ray stop

ray-status:
	ray status
