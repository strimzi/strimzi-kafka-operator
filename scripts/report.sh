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
	"replicasets"
	"configmaps"
	"secrets"
	"services"
	"poddisruptionbudgets"
	"roles"
	"rolebindings"
	"networkpolicies"
	"routes"
	"pods"
	"persistentvolumeclaims"
	)

nonnamespaced_resources_to_fetch=(
	"clusterroles"
	"clusterrolebindings"
	)

get_namespaced_yamls() {
	mkdir -p $direct/reports/"$1"
	resources=$($platform get $1 -l strimzi.io/cluster=$cluster -o name -n $namespace)
	echo "$1"
	for line in $resources; do
		filename=`echo $line | cut -f 2 -d "/"`
		echo "   "$line
		$platform get $line -o yaml -n $namespace | sed 's/^\(\s*password\s*:\s*\).*/\1*****/' \
		| sed 's/^\(\s*.*\.key\s*:\s*\).*/\1*****/' > $direct/reports/"$1"/"$filename".yaml
	done;
}

for res in "${resources_to_fetch[@]}"; do
	get_namespaced_yamls "$res"
done;

get_nonnamespaced_yamls() {
	mkdir -p $direct/reports/"$1"
	resources=$($platform get $1 -l app=strimzi -o name -n $namespace)
	echo "$1"
	for line in $resources; do
		filename=`echo $line | cut -f 2 -d "/"`
		echo "   "$line
		$platform get $line -o yaml | sed 's/^\(\s*password\s*:\s*\).*/\1*****/' \
		| sed 's/^\(\s*.*\.key\s*:\s*\).*/\1*****/' > $direct/reports/"$1"/"$filename".yaml
	done;
}

for res in "${nonnamespaced_resources_to_fetch[@]}"; do
	get_nonnamespaced_yamls "$res"
done;

mkdir -p $direct/reports/podLogs
mkdir -p $direct/reports/configs

echo "Pod logs:"
pods=$($platform get pods -l strimzi.io/cluster=$cluster -o name -n $namespace | cut -d "/" -f 2)
for pod in $pods; do
	echo "   "$pod
	if [[ $pod == *"-entity-operator-"* ]]; then
	  $platform logs $pod -c tls-sidecar -n $namespace > $direct/reports/podLogs/"$pod"-tls-sidecar.log
	  $platform logs $pod -c topic-operator -n $namespace > $direct/reports/podLogs/"$pod"-topic-operator.log
	  $platform logs $pod -c user-operator -n $namespace > $direct/reports/podLogs/"$pod"-user-operator.log
	  $platform logs $pod -p -c tls-sidecar -n $namespace 2>/dev/null > $direct/reports/podLogs/previous-"$pod"-tls-sidecar.log
	  $platform logs $pod -p -c topic-operator -n $namespace 2>/dev/null > $direct/reports/podLogs/previous-"$pod"-topic-operator.log
	  $platform logs $pod -p -c user-operator -n $namespace 2>/dev/null > $direct/reports/podLogs/previous-"$pod"-user-operator.log
	fi
	if [[ $pod == *"-kafka-"* ]]; then
	  $platform logs $pod -c tls-sidecar -n $namespace > $direct/reports/podLogs/"$pod"-tls-sidecar.log
	  $platform logs $pod -c kafka -n $namespace > $direct/reports/podLogs/"$pod"-kafka.log
	  $platform logs $pod -p -c tls-sidecar -n $namespace 2>/dev/null > $direct/reports/podLogs/previous-"$pod"-tls-sidecar.log
	  $platform logs $pod -p -c kafka -n $namespace 2>/dev/null > $direct/reports/podLogs/previous-"$pod"-kafka.log

	  $platform exec -i $pod -n $namespace -c kafka -- cat /tmp/strimzi.properties > $direct/reports/configs/"$pod".cfg
	fi
	if [[ $pod == *"-zookeeper-"* ]]; then
	  $platform logs $pod -c tls-sidecar -n $namespace > $direct/reports/podLogs/"$pod"-tls-sidecar.log
	  $platform logs $pod -c zookeeper -n $namespace > $direct/reports/podLogs/"$pod"-zookeeper.log
	  $platform logs $pod -p -c tls-sidecar -n $namespace 2>/dev/null > $direct/reports/podLogs/previous-"$pod"-tls-sidecar.log
	  $platform logs $pod -p -c zookeeper -n $namespace 2>/dev/null > $direct/reports/podLogs/previous-"$pod"-zookeeper.log

	  $platform exec -i $pod -n $namespace -c zookeeper -- cat /tmp/zookeeper.properties > $direct/reports/configs/"$pod".cfg
	fi
done;

# getting CO deployment from the pod
co_pod=$($platform get pod -l strimzi.io/kind=cluster-operator -o name -n $namespace)
$platform get pod -l strimzi.io/kind=cluster-operator -o yaml -n $namespace > $direct/reports/deployments/cluster-operator.yaml
$platform logs $co_pod -n $namespace > $direct/reports/podLogs/cluster-operator.yaml
$platform logs $co_pod -p -n $namespace 2>/dev/null > $direct/reports/podLogs/previous-cluster-operator.yaml

# getting CO replicaset
co_rs=$($platform get replicaset -l strimzi.io/kind=cluster-operator -o name -n $namespace)
$platform describe $co_rs -n $namespace > $direct/reports/replicasets/cluster-operator-replicaset.yaml

#Kafka, KafkaConnect, KafkaConnectS2i, KafkaTopic, KafkaUser, KafkaMirrorMaker
echo "CRs:"
mkdir -p $direct/reports/crs
crs=$($platform get crd -o name)
for line in $crs; do
	cr=`echo $line | cut -d "/" -f 2`
	resources=$($platform get $cr -o name -n $namespace | cut -d "/" -f 2)
	if ! [[ -z "$resources" &&  $cr == *"kafka.strimzi.io" ]]; then
		echo $cr
		for line in $resources; do
			resource=`echo $line | cut -f 1 -d " "`
			$platform get $cr $resource -o yaml > $direct/reports/crs/"$cr"-"$resource".yaml
			echo "   "$resource
		done;
	fi
done;

echo "CRDs:"
mkdir -p $direct/reports/crds
crds=$($platform get crd -o name)
for line in $crds; do
	crd=`echo $line | cut -d "/" -f 2`
	if [[ $crd == *"kafka.strimzi.io" ]]; then
		echo "   "$crd
		$platform get crd $crd -o yaml > $direct/reports/crds/"$crd".yaml
	fi
done;

mkdir -p $direct/reports/events
$platform get event -n $namespace > /$direct/reports/events/events.yaml

filename=`date +"%d-%m-%Y_%H-%M-%S"`
filename=report-"$filename"
zip -qr $filename.zip /$direct/reports/
echo "Report file $filename.zip created"