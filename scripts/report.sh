#!/bin/bash

oc_installed=false
kubectl_installed=false
platform="kubectl"

oc >/dev/null

if [ $? -eq 0 ]; then
	oc_installed=true
	platform="oc"
fi

kubectl >/dev/null

if [ $? -eq 0 ]; then # we will use kubectl with priority (?)
	kubectl_installed=true
	platform="kubectl"
fi

if [[ $oc_installed = false && $kubectl_installed = false ]]; then
	echo "There is no oc or kubectl installed"
	exit 1
fi

# default values
cluster="my-cluster"
namespace="myproject"

usage() {
	echo "Usage: $0 [--namespace <string>] [--cluster <string>]" 1>&2; 
	echo "Default values namespace=my-project cluster=my-cluster" 1>&2; 
	exit 1; 
}

optspec=":-:"
while getopts "$optspec" optchar; do
    case "${optchar}" in
        -)
            case "${OPTARG}" in
                cluster=*)
                    cluster=${OPTARG#*=}
                    ;;
                namespace=*)
                    namespace=${OPTARG#*=}
                    ;;
                *)
                    usage
                    ;;
            esac;;
        *)
            cluster="my-cluster"
            namespace="myproject"
            ;;
    esac
done
shift $((OPTIND-1))

direct=`mktemp -d`
resources_to_fetch=(
	"deployments"
	"statefulsets"
	"configmaps"
	"secrets"
	"services"
	"poddisruptionbudgets"
	"roles"
	"clusterroles"
	"rolebindings"
	"clusterrolebindings"
	"networkpolicies"
	"routes"
	)

get_yamls() {
	mkdir -p $direct/reports/"$1"
	resources=`eval "$platform get $1 -l strimzi.io/cluster=$cluster -o name -n $namespace"`
	echo "$1"
	while read -r line; do
		if ! [ -z $line ]; then
			filename=`echo $line | cut -f 2 -d "/"`
			echo "   "$line
			eval "$platform get $line -o yaml -n $namespace | sed 's/^\(\s*password\s*:\s*\).*/\1*****/' \
			| sed 's/^\(\s*.*\.key\s*:\s*\).*/\1*****/' >> $direct/reports/"$1"/"$filename".yaml"
		fi
	done <<< $resources;
}

for res in "${resources_to_fetch[@]}"; do
	get_yamls "$res"
done

mkdir -p $direct/reports/podLogs
mkdir -p $direct/reports/configs

echo "Pods:"
pods=`eval "$platform get pods -l strimzi.io/cluster=$cluster -o name -n $namespace | cut -d "/" -f 2"`
for pod in $pods; do
	echo "   "$pod
	if [[ $pod == *"-entity-operator-"* ]]; then
	  eval "$platform logs $pod -c tls-sidecar -n $namespace >> $direct/reports/podLogs/"$pod"-tls-sidecar.log"
	  eval "$platform logs $pod -c topic-operator -n $namespace >> $direct/reports/podLogs/"$pod"-topic-operator.log"
	  eval "$platform logs $pod -c user-operator -n $namespace >> $direct/reports/podLogs/"$pod"-user-operator.log"
	  eval "$platform logs $pod -p -c tls-sidecar -n $namespace >> $direct/reports/podLogs/previous-"$pod"-tls-sidecar.log"
	  eval "$platform logs $pod -p -c topic-operator -n $namespace >> $direct/reports/podLogs/previous-"$pod"-topic-operator.log"
	  eval "$platform logs $pod -p -c user-operator -n $namespace >> $direct/reports/podLogs/previous-"$pod"-user-operator.log"
	fi
	if [[ $pod == *"-kafka-"* ]]; then
	  eval "$platform logs $pod -c tls-sidecar -n $namespace >> $direct/reports/podLogs/"$pod"-tls-sidecar.log"
	  eval "$platform logs $pod -c kafka -n $namespace >> $direct/reports/podLogs/"$pod"-kafka.log"
	  eval "$platform logs $pod -p -c tls-sidecar -n $namespace >> $direct/reports/podLogs/previous-"$pod"-tls-sidecar.log"
	  eval "$platform logs $pod -p -c kafka -n $namespace >> $direct/reports/podLogs/previous-"$pod"-kafka.log"

	  eval "$platform exec -i $pod -n $namespace -c kafka -- cat /tmp/strimzi.properties >> $direct/reports/configs/"$pod".cfg"
	fi
	if [[ $pod == *"-zookeeper-"* ]]; then
	  eval "$platform logs $pod -c tls-sidecar -n $namespace >> $direct/reports/podLogs/"$pod"-tls-sidecar.log"
	  eval "$platform logs $pod -c zookeeper -n $namespace >> $direct/reports/podLogs/"$pod"-zookeeper.log"
	  eval "$platform logs $pod -p -c tls-sidecar -n $namespace >> $direct/reports/podLogs/previous-"$pod"-tls-sidecar.log"
	  eval "$platform logs $pod -p -c zookeeper -n $namespace >> $direct/reports/podLogs/previous-"$pod"-zookeeper.log"

	  eval "$platform exec -i $pod -n $namespace -c zookeeper -- cat /tmp/zookeeper.properties >> $direct/reports/configs/"$pod".cfg"
	fi
	if [[ $pod == *"-cluster-operator-"* ]]; then
	  eval "$platform logs $pod -n $namespace >> $direct/reports/podLogs/"$pod".log"
	  eval "$platform logs -p $pod -n $namespace >> $direct/reports/podLogs/previous-"$pod".log"
	fi
done;

#Kafka, KafkaConnect, KafkaConnectS2i, KafkaTopic, KafkaUser, KafkaMirrorMaker
mkdir -p $direct/reports/crs
crs=`eval "$platform get crd -o name"`
while read -r line; do
	cr=`echo $line | cut -d "/" -f 2`
	echo $cr
	resources=`eval "$platform get $cr -l strimzi.io/cluster=$cluster -o name -n $namespace | cut -d "/" -f 2"`
	if ! [[ -z "$resources" &&  $cr == *"kafka.strimzi.io" ]]; then
		while read -r line; do
			resource=`echo $line | cut -f 1 -d " "`
			eval "$platform get $cr $resource -o yaml -n $namespace >> $direct/reports/crs/"$cr"-"$resource".yaml"
			echo "   "$resource
		done <<< $resources;
	fi
done <<< $crs;

mkdir -p $direct/reports/events
eval "$platform get event -n $namespace >> $direct/reports/events/events.yaml"

filename=`date +"%Y-%m-%d_%H-%M-%S"`
filename=report-"$filename".zip
zip -qr $filename.zip $direct/reports
echo "Report file $filename created"