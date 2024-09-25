build:
	docker buildx build --platform linux/amd64 --load -t fleet-telemetry-consumer .

run:
	docker compose up --build