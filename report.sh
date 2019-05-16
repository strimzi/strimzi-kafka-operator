#!/bin/bash
echo "Enter cluster name (default my-cluster)"
read cluster
if [ -z $cluster ]; then
	cluster="my-cluster"
fi

get_yamls() {
	mkdir -p ./reports/"$1"s
	resources=`oc get "$1"s  -l strimzi.io/cluster=$cluster | tail -n +2` # remove legend
	while read -r line; do
		resource=`echo $line | cut -f 1 -d " "`
		echo $resource
		oc get "$1" $resource -o yaml >> ./reports/"$1"s/"$resource".txt
	done <<< $resources;
}

get_yamls "deployment"
get_yamls "statefulset"
get_yamls "configmap"
get_yamls "secret"

mkdir -p ./reports/podLogs
pods=`oc get pods -l strimzi.io/cluster=$cluster | tail -n +2` # remove legend
while read -r line; do
	pod=`echo $line | cut -f 1 -d " "`
	echo $pod
	if [[ $pod == *"-entity-operator-"* ]]; then
	  oc logs $pod -c tls-sidecar >> ./reports/podLogs/"$pod"-tls-sidecar.txt
	  oc logs $pod -c topic-operator >> ./reports/podLogs/"$pod"-topic-operator.txt
	  oc logs $pod -c user-operator >> ./reports/podLogs/"$pod"-user-operator.txt
	fi
	if [[ $pod == *"-kafka-"* ]]; then
	  oc logs $pod -c tls-sidecar >> ./reports/podLogs/"$pod"-tls-sidecar.txt
	  oc logs $pod -c kafka >> ./reports/podLogs/"$pod"-kafka.txt
	fi
	if [[ $pod == *"-zookeeper-"* ]]; then
	  oc logs $pod -c tls-sidecar >> ./reports/podLogs/"$pod"-tls-sidecar.txt
	  oc logs $pod -c zookeeper >> ./reports/podLogs/"$pod"-zookeeper.txt
	fi
	if [[ $pod == *"-cluster-operator-"* ]]; then
	  oc logs $pod >> ./reports/podLogs/"$pod"-cluster-operator.txt
	fi
done <<< $pods;


#Kafka, KafkaConnect, KafkaConnectS2i, KafkaTopic, KafkaUser, KafkaMirrorMaker
mkdir -p ./reports/crs

crs=`oc get crd | tail -n +2` # remove legend
while read -r line; do
	cr=`echo $line | cut -f 1 -d " "`
	echo $cr
	resources=`oc get $cr -l strimzi.io/cluster=$cluster | tail -n +2` # remove legend
	if ! [[ -z "$resources" &&  $cr == *"kafka.strimzi.io" ]]; then
		while read -r line; do
			resource=`echo $line | cut -f 1 -d " "`
			oc get $cr $resource -o yaml >> ./reports/crs/"$cr"-"$resource".txt
			echo "   "$resource
		done <<< $resources;
	fi
done <<< $crs;

zip -qr report.zip reports
rm -rf reports

echo "Done"