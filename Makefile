build:
	docker buildx build --platform linux/amd64 --load -t fleet-telemetry-consumer .

run:
	docker compose up --build

local:
	export AWS_ACCESS_KEY_ID="71L86RUJQ0N8F1KSHFZ1"
	export AWS_BUCKET_HOST="rook-ceph-rgw-my-store.rook-ceph.svc.cluster.local"
	export AWS_BUCKET_NAME="ceph-tesla"
	export AWS_BUCKET_PORT="80"
	export AWS_BUCKET_PROTOCOL="http"
	export AWS_BUCKET_REGION="us-east-1"
	export AWS_ENABLED="true"
	export AWS_SECRET_ACCESS_KEY="pUjngEUP3RlqGZ7l7YVoT7XMiX3olvSzGHVfxjtl"
	export KAFKA_AUTO_OFFSET_RESET="earliest"
	export KAFKA_BOOTSTRAP_SERVERS="tesla-kafka-bootstrap.tesla.svc.cluster.local:9092"
	export KAFKA_GROUP_ID="fleet-telemetry-consumer"
	export KAFKA_TOPIC="tesla_V"
	export LOAD_DAYS="1"
	export LOCAL_BASE_PATH="/data"
	export LOCAL_ENABLED="false"
	export POSTGRES_DBNAME="app"
	export POSTGRES_ENABLED="true"
	export POSTGRES_HOST="postgres-rw.tesla.svc.cluster.local"
	export POSTGRES_PASSWORD="uFSDfWdCpjuPa6xqiBQ3HiFZNxG5zbpca1NE9SdZe75N8NKZCRdnU9uqA3FVArxh"
	export POSTGRES_PORT="5432"
	export POSTGRES_SSLMODE="disable"
	export POSTGRES_USER="app"
	export PROMETHEUS_ADDR=":2112"
	go run main.go