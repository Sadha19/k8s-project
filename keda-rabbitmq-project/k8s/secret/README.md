kubectl create namespace messaging --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic rabbitmq-credentials -n messaging \
  --from-literal=username=appuser \
  --from-literal=password="$(openssl rand -hex 24)"

kubectl create secret generic rabbitmq-scaler-connection -n messaging \
  --from-literal=host="amqp://appuser:<SAME_PASSWORD>@rabbitmq.messaging.svc.cluster.local:5672/"

kubectl create secret docker-registry docker-rs -n messaging \
  --docker-server=myregistry.com:5000 \
  --docker-username=<USER> \
  --docker-password=<NEW_PASSWORD>
