build:
	docker build . --platform linux/amd64 --no-cache

run:
	docker compose up --build