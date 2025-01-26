apply:
	kustomize build examples/kustomization | kubectl apply -f -

delete:
	kustomize build examples/kustomization | kubectl delete -f -

reload:
	kubectl delete namespace tesla
	kubectl create namespace tesla
	make apply

run:
	make proxy
	docker compose up --force-recreate --build

local:
	make proxy
	docker compose down
	go run main.go
proxy:
	docker compose up --force-recreate tesla-http-proxy -d

pull-secrets:
	@echo "Creating static directory if it doesn't exist..."
	@mkdir -p ./static
	@echo "Pulling secrets from tesla-fleet-api..."
	kubectl get secret tesla-fleet-api -n tesla -o jsonpath='{.data}' | jq -r 'to_entries[] | "echo \"Extracting \(.key)...\"; echo \(.value) | base64 -d > \"./static/\(.key)\""' | sh
	@echo "Pulling secrets from tesla-raj-tls..."
	kubectl get secret tesla-raj-tls -n tesla -o jsonpath='{.data}' | jq -r 'to_entries[] | "echo \"Extracting \(.key)...\"; echo \(.value) | base64 -d > \"./static/\(.key)\""' | sh
	@echo "Done pulling secrets!"

.PHONY: apply delete reload run proxy pull-secrets


