#!/bin/bash
DIR=`pwd`
k=false

usage() { echo "Usage: $0 [-c <cluster>] -n <namespace>"
		  echo " When -c is used, Kafka cluster with metrics is deployed."
		  1>&2; exit 1;}

while getopts "n:c:" o; do
    case "${o}" in
        n)  NAMESPACE=${OPTARG}
        	;;
        c)  c=true
            CLUSTER=${OPTARG}
        	;; 	
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${NAMESPACE}" ] || [[ "$c" == true && -z "${CLUSTER}" ]] ; then
    usage
fi

if [ "$c" == true ] ; then
	# Determine whether CO is deployed in specified NS
	lines_of_operator=`kubectl get pods -l strimzi.io/kind=cluster-operator -n $NAMESPACE | wc -l`

	if [ "$lines_of_operator" -lt "2" ]; then
		echo "Cluster Operator is not deployed!"
		exit 1
	fi

	echo "Deploying Kafka "$CLUSTER
	if [[ "$OSTYPE" == "linux-gnu" ]]; then
	    sed "s/my-cluster/$CLUSTER/" $DIR/metrics/examples/kafka/kafka-metrics.yaml > $DIR/metrics/examples/kafka/$CLUSTER-kafka-metrics.yaml
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        sed '' "s/my-cluster/$CLUSTER/" $DIR/metrics/examples/kafka/kafka-metrics.yaml > $DIR/metrics/examples/kafka/$CLUSTER-kafka-metrics.yaml
    fi
	kubectl apply -f $DIR/metrics/examples/kafka/$CLUSTER-kafka-metrics.yaml -n $NAMESPACE

	# Delay for allowing cluster operator to create the Zookeeper statefulset
	sleep 5

	zkReplicas=$(kubectl get kafka $CLUSTER -o jsonpath="{.spec.zookeeper.replicas}" -n $NAMESPACE)
	echo "Waiting for Zookeeper cluster to be ready..."
	readyReplicas="0"
	while [ "$readyReplicas" != "$zkReplicas" ]
	do
	    sleep 2
	    readyReplicas=$(kubectl get statefulsets $CLUSTER-zookeeper -o jsonpath="{.status.readyReplicas}" -n $NAMESPACE)
	done
	echo "...Zookeeper cluster ready"

	# Delay for allowing cluster operator to create the Kafka statefulset
	sleep 5

	kReplicas=$(kubectl get kafka $CLUSTER -o jsonpath="{.spec.kafka.replicas}" -n $NAMESPACE)
	echo "Waiting for Kafka cluster to be ready..."
	readyReplicas="0"
	while [ "$readyReplicas" != "$kReplicas" ]
	do
	    sleep 2
	    readyReplicas=$(kubectl get statefulsets $CLUSTER-kafka -o jsonpath="{.status.readyReplicas}" -n $NAMESPACE)
	done
	echo "...Kafka cluster ready"

	echo "Waiting for entity operator to be ready..."
	kubectl rollout status deployment/$CLUSTER-entity-operator -w -n $NAMESPACE
	echo "...entity operator ready"

	rm $DIR/metrics/examples/kafka/$CLUSTER-kafka-metrics.yaml

fi

# Not deploying Kafka
# Deploy Prometheus operator
curl https://gist.githubusercontent.com/stanlyDoge/5a0b43114d9be0e72f0f4b4e3ee168a1/raw/09614357fa20efd14deffb31a06fd87aaa6dc6c8/prometheus-operator.yaml --output $DIR/metrics/prometheus-operator.yaml

if [[ "$OSTYPE" == "linux-gnu" ]]; then
    sed "s/namespace: .*/namespace: $NAMESPACE/" $DIR/metrics/prometheus-operator.yaml > $DIR/metrics/prometheus-operator-deploy.yaml
elif [[ "$OSTYPE" == "darwin"* ]]; then
    sed '' "s/namespace: .*/namespace: $NAMESPACE/" $DIR/metrics/prometheus-operator.yaml > $DIR/metrics/prometheus-operator-deploy.yaml
fi
kubectl apply -f $DIR/metrics/prometheus-operator-deploy.yaml -n $NAMESPACE

echo "Waiting for Prometheus operator to be ready..."
kubectl rollout status deployment/strimzi-prometheus-operator -w -n $NAMESPACE
echo "...Prometheus operator ready"

rm $DIR/metrics/prometheus-operator-deploy.yaml

kubectl create secret generic additional-scrape-configs --from-file=$DIR/metrics/examples/prometheus/additional-properties/prometheus-additional.yaml
kubectl create secret generic alertmanager-alertmanager --from-file=alertmanager.yaml=$DIR/metrics/examples/prometheus/alertmanager-config/alert-manager-config.yaml


if [[ "$OSTYPE" == "linux-gnu" ]]; then
    sed "s/namespace: .*/namespace: $NAMESPACE/" $DIR/metrics/examples/prometheus/install/prometheus.yaml > $DIR/metrics/examples/prometheus/install/prometheus-deploy.yaml
elif [[ "$OSTYPE" == "darwin"* ]]; then
    sed '' "s/namespace: .*/namespace: $NAMESPACE/" $DIR/metrics/examples/prometheus/install/prometheus.yaml > $DIR/metrics/examples/prometheus/install/prometheus-deploy.yaml
fi

kubectl apply -f $DIR/metrics/examples/prometheus/install/prometheus-rules.yaml -n $NAMESPACE
kubectl apply -f $DIR/metrics/examples/prometheus/install/prometheus-deploy.yaml -n $NAMESPACE
rm $DIR/metrics/examples/prometheus/install/prometheus-deploy.yaml
kubectl apply -f $DIR/metrics/examples/prometheus/install/strimzi-service-monitor.yaml -n $NAMESPACE
kubectl apply -f $DIR/metrics/examples/prometheus/install/alert-manager.yaml -n $NAMESPACE

# Delay for allowing Prometheus operator to create the Alert manager statefulset
sleep 15

amReplicas=$(kubectl get statefulsets alertmanager-alertmanager -o jsonpath="{.spec.replicas}" -n $NAMESPACE)
echo "Waiting for Alert manager to be ready..."
readyReplicas="0"
while [ "$readyReplicas" != "$amReplicas" ]
do
    sleep 2
    readyReplicas=$(kubectl get statefulsets alertmanager-alertmanager -o jsonpath="{.status.readyReplicas}" -n $NAMESPACE)
done
echo "...Alert manager ready"

# Delay for allowing Prometheus operator to create the Prometheus statefulset
sleep 15

pReplicas=$(kubectl get statefulsets prometheus-prometheus -o jsonpath="{.spec.replicas}" -n $NAMESPACE)
echo "Waiting for Prometheus to be ready..."
readyReplicas="0"
while [ "$readyReplicas" != "$pReplicas" ]
do
    sleep 2
    readyReplicas=$(kubectl get statefulsets prometheus-prometheus -o jsonpath="{.status.readyReplicas}" -n $NAMESPACE)
done
echo "...Prometheus ready"

rm $DIR/metrics/prometheus-operator.yaml

# Grafana
kubectl apply -f $DIR/metrics/examples/grafana/grafana.yaml -n $NAMESPACE

echo "Waiting for Grafana server to be ready..."
kubectl rollout status deployment/grafana -w -n $NAMESPACE
echo "...Grafana server ready"

kubectl expose service/grafana -n $NAMESPACE

sleep 5

# Posting Prometheus datasource and dashboards to Grafana
curl -X POST http://admin:admin@$(kubectl get routes grafana -o jsonpath='{.status.ingress[0].host}{"\n"}')/api/datasources  -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"name":"Prometheus","isDefault":true ,"type":"prometheus","url":"http://prometheus:9090","access":"proxy","basicAuth":false}'

# We have to deploy dashboards from '$DIR/metrics/examples/grafana/strimzi-kafka.json' manually because the files are missing "dashboard" element