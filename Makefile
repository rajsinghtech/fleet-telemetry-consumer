build:
	docker buildx build --platform linux/amd64 --load -t fleet-telemetry-consumer .

run:
	docker compose up --build

local:
	export AWS_ACCESS_KEY_ID="7JPPOUCUV401UJB27N0D"
	export AWS_BUCKET_HOST="rook-ceph-rgw-hybrid-objectstore.rook-ceph.svc.cluster.local"
	export AWS_BUCKET_NAME="ceph-hybrid-tesla"
	export AWS_BUCKET_PORT="80"
	export AWS_BUCKET_PROTOCOL="http"
	export AWS_BUCKET_REGION="us-east-1"
	export AWS_ENABLED="true"
	export AWS_SECRET_ACCESS_KEY="pgsqnE8WE90CsiXwSx6kBz91vjcaIfn1ko7GzBbw"
	export KAFKA_AUTO_OFFSET_RESET="earliest"
	export KAFKA_BOOTSTRAP_SERVERS="tesla-kafka-bootstrap.tesla.svc.cluster.local:9092"
	export KAFKA_GROUP_ID="fleet-telemetry-consumer"
	export KAFKA_TOPIC="tesla_V"
	export KAFKA_ENABLED="true"
	export LOAD_DAYS="1"
	export LOCAL_BASE_PATH="/data"
	export LOCAL_ENABLED="false"
	export POSTGRES_DBNAME="app"
	export POSTGRES_ENABLED="true"
	export POSTGRES_HOST="postgres-rw.tesla.svc.cluster.local"
	export POSTGRES_PASSWORD="JFFaDSpyrfnvKYX1qAbaXA6GmmvCFi6QV9vkBhca7r40sUYdFjd3vBM7MwyLET5W"
	export POSTGRES_PORT="5432"
	export POSTGRES_SSLMODE="disable"
	export POSTGRES_USER="app"
	go run main.go