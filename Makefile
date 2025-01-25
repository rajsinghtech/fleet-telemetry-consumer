apply:
	kustomize build examples/kustomization | kubectl apply -f -

delete:
	kustomize build examples/kustomization | kubectl delete -f -

reload:
	kubectl delete namespace tesla
	kubectl create namespace tesla
	make apply
