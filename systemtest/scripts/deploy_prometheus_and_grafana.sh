#!/bin/bash
DIR=`pwd`
k=false

usage() { echo "Usage: $0 [-k] -n <namespace> -c <cluster>"  
		  echo " When -k is used, Kafka cluster with metrics is deployed."
		  1>&2; exit 1;}

while getopts "kn:c:" o; do
    case "${o}" in
        k)
            k=true
            ;;
        n)  NAMESPACE=${OPTARG}
        	;;
        c)  CLUSTER=${OPTARG}
        	;; 	
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${NAMESPACE}" ] || [[ "$k" == true && -z "${CLUSTER}" ]] ; then
    usage
fi

if [ "$k" == true ] ; then
	# Determine whether CO is deployed in specified NS
	lines_of_operator=`oc get pods -l strimzi.io/kind=cluster-operator -n $NAMESPACE | wc -l`

	if [ "$lines_of_operator" -lt "2" ]; then
		echo "Cluster Operator is not deployed!"
		exit 1
	fi

	echo "Deploying Kafka "$CLUSTER
	sed "s/my-cluster/$CLUSTER/" $DIR/metrics/examples/kafka/kafka-metrics.yaml > $DIR/metrics/examples/kafka/$CLUSTER-kafka-metrics.yaml

	oc apply -f $DIR/metrics/examples/kafka/$CLUSTER-kafka-metrics.yaml -n $NAMESPACE

	# Delay for allowing cluster operator to create the Zookeeper statefulset
	sleep 5

	zkReplicas=$(oc get kafka $CLUSTER -o jsonpath="{.spec.zookeeper.replicas}" -n $NAMESPACE)
	echo "Waiting for Zookeeper cluster to be ready..."
	readyReplicas="0"
	while [ "$readyReplicas" != "$zkReplicas" ]
	do
	    sleep 2
	    readyReplicas=$(oc get statefulsets $CLUSTER-zookeeper -o jsonpath="{.status.readyReplicas}" -n $NAMESPACE)
	done
	echo "...Zookeeper cluster ready"

	# Delay for allowing cluster operator to create the Kafka statefulset
	sleep 5

	kReplicas=$(oc get kafka $CLUSTER -o jsonpath="{.spec.kafka.replicas}" -n $NAMESPACE)
	echo "Waiting for Kafka cluster to be ready..."
	readyReplicas="0"
	while [ "$readyReplicas" != "$kReplicas" ]
	do
	    sleep 2
	    readyReplicas=$(oc get statefulsets $CLUSTER-kafka -o jsonpath="{.status.readyReplicas}" -n $NAMESPACE)
	done
	echo "...Kafka cluster ready"

	echo "Waiting for entity operator to be ready..."
	oc rollout status deployment/$CLUSTER-entity-operator -w -n $NAMESPACE
	echo "...entity operator ready"

	rm $DIR/metrics/examples/kafka/$CLUSTER-kafka-metrics.yaml

fi

# Not deploying Kafka
# Deploy Prometheus operator
curl https://gist.githubusercontent.com/stanlyDoge/5a0b43114d9be0e72f0f4b4e3ee168a1/raw/09614357fa20efd14deffb31a06fd87aaa6dc6c8/prometheus-operator.yaml --output $DIR/metrics/prometheus-operator.yaml

sed "s/namespace: .*/namespace: $NAMESPACE/" $DIR/metrics/prometheus-operator.yaml > $DIR/metrics/prometheus-operator-deploy.yaml
oc apply -f $DIR/metrics/prometheus-operator-deploy.yaml -n $NAMESPACE

echo "Waiting for Prometheus operator to be ready..."
oc rollout status deployment/strimzi-prometheus-operator -w -n $NAMESPACE
echo "...Prometheus operator ready"

rm $DIR/metrics/prometheus-operator-deploy.yaml

oc create secret generic additional-scrape-configs --from-file=$DIR/metrics/examples/prometheus/additional-properties/prometheus-additional.yaml
oc create secret generic alertmanager-alertmanager --from-file=alertmanager.yaml=$DIR/metrics/examples/prometheus/alertmanager-config/alert-manager-config.yaml

sed "s/namespace: .*/namespace: $NAMESPACE/" $DIR/metrics/examples/prometheus/install/prometheus.yaml > $DIR/metrics/examples/prometheus/install/prometheus-deploy.yaml

oc apply -f $DIR/metrics/examples/prometheus/install/prometheus-rules.yaml -n $NAMESPACE
oc apply -f $DIR/metrics/examples/prometheus/install/prometheus-deploy.yaml -n $NAMESPACE
rm $DIR/metrics/examples/prometheus/install/prometheus-deploy.yaml
oc apply -f $DIR/metrics/examples/prometheus/install/strimzi-service-monitor.yaml -n $NAMESPACE
oc apply -f $DIR/metrics/examples/prometheus/install/alert-manager.yaml -n $NAMESPACE

# Delay for allowing Prometheus operator to create the Alert manager statefulset
sleep 10

amReplicas=$(oc get statefulsets alertmanager-alertmanager -o jsonpath="{.spec.replicas}" -n $NAMESPACE)
echo "Waiting for Alert manager to be ready..."
readyReplicas="0"
while [ "$readyReplicas" != "$amReplicas" ]
do
    sleep 2
    readyReplicas=$(oc get statefulsets alertmanager-alertmanager -o jsonpath="{.status.readyReplicas}" -n $NAMESPACE)
done
echo "...Alert manager ready"

# Delay for allowing Prometheus operator to create the Prometheus statefulset
sleep 5

pReplicas=$(oc get statefulsets prometheus-prometheus -o jsonpath="{.spec.replicas}" -n $NAMESPACE)
echo "Waiting for Prometheus to be ready..."
readyReplicas="0"
while [ "$readyReplicas" != "$pReplicas" ]
do
    sleep 2
    readyReplicas=$(oc get statefulsets prometheus-prometheus -o jsonpath="{.status.readyReplicas}" -n $NAMESPACE)
done
echo "...Prometheus ready"

rm $DIR/metrics/prometheus-operator.yaml

# Grafana
oc apply -f $DIR/metrics/examples/grafana/grafana.yaml -n $NAMESPACE
oc expose service/grafana -n $NAMESPACE

echo "Waiting for Grafana server to be ready..."
oc rollout status deployment/grafana -w -n $NAMESPACE
echo "...Grafana server ready"

sleep 5

# Posting Prometheus datasource and dashboards to Grafana
curl -X POST http://admin:admin@$(oc get routes grafana -o jsonpath='{.status.ingress[0].host}{"\n"}')/api/datasources  -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"name":"Prometheus","isDefault":true ,"type":"prometheus","url":"http://prometheus:9090","access":"proxy","basicAuth":false}'

# We have to deploy these dashboards manualy because the files are missing "dashboard" element
#curl -X POST http://admin:admin@$(oc get routes grafana -o jsonpath='{.status.ingress[0].host}{"\n"}')/api/dashboards/import -d @$DIR/metrics/examples/grafana/strimzi-kafka.json --header "Content-Type: application/json"
#curl -X POST http://admin:admin@$(oc get routes grafana -o jsonpath='{.status.ingress[0].host}{"\n"}')/api/dashboards/import -d @$DIR/metrics/examples/grafana/strimzi-zookeeper.json --header "Content-Type: application/json"
#curl -X POST http://admin:admin@$(oc get routes grafana -o jsonpath='{.status.ingress[0].host}{"\n"}')/api/dashboards/import -d @$DIR/metrics/examples/grafana/strimzi-kafka-connect.json --header "Content-Type: application/json"
