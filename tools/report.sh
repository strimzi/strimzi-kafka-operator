#!/usr/bin/env bash

oc_installed=false
kubectl_installed=false
platform="kubectl"
secrets="hidden"

oc &>/dev/null

if [ $? -eq 0 ]; then
	oc_installed=true
	platform="oc"
fi

kubectl &>/dev/null

if [ $? -eq 0 ]; then # we will use kubectl with priority (?)
	kubectl_installed=true
	platform="kubectl"
fi

if [[ $oc_installed = false && $kubectl_installed = false ]]; then
	echo "There is no oc or kubectl installed"
	exit 1
fi


usage() {
	echo "Usage: $0 --namespace=<string> --cluster=<string> --secrets=(off|hidden|all)" 1>&2;
	echo "Default level of secret verbosity is 'hidden' (only secret key will be reported)."
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
                secrets=*)
                    secrets=${OPTARG#*=}
                    ;;
                *)
                    usage
                    ;;
            esac;;
    esac
done
shift $((OPTIND-1))

if [ -z $cluster ] && [ -z $namespace ]; then
   echo "--cluster and --namespace are mandatory options."
   usage
fi

if [ -z $secrets ]; then
  secrets="hidden"
fi

if [ "$secrets" != "all" ] && [ "$secrets" != "off" ] && [ "$secrets" != "hidden" ]; then
  echo "Unknown secrets verbosity level. Use one of 'off', 'hidden' or 'all'."
  echo " 'all' - secret keys and data values are reported"
  echo " 'hidden' - secrets with only data keys are reported"
  echo " 'off' - secrets are not reported at all"
  echo "Default value is 'hidden'"
  usage
fi

if [ -z $cluster ]; then
   echo "Cluster was not specified. Use --cluster option to specify it."
   usage
fi

if [ -z $namespace ]; then
   echo "Namespace was not specified. Use --namespace option to specify it."
   usage
fi

$platform get ns $namespace &> /dev/null

if [ $? = "1" ]; then
	echo "Namespace $namespace not found! Exiting"
	exit 1
fi

$platform get kafka $cluster -n $namespace &> /dev/null

if [ $? = "1" ]; then
	echo "Kafka cluster $cluster in namespace $namespace not found! Exiting"
	exit 1
fi

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

if [ "$secrets" = "off" ]; then
  resources_to_fetch=( "${resources_to_fetch[@]/secrets}" )
fi

get_masked_secrets() {
	mkdir -p $direct/reports/"$1"
	echo "$1"
	resources=$($platform get $1 -l strimzi.io/cluster=$cluster -o name -n $namespace)
	for line in $resources; do
		filename=`echo $line | cut -f 2 -d "/"`
		echo "   "$line
		original_data=`oc get $line -o=jsonpath='{.data}' -n $namespace | cut -c5-`
		SAVEIFS=$IFS
    IFS=$'\n'
    original_data=($original_data)
    IFS=$SAVEIFS

		data_entries=()
		for data in $original_data; do
		  entry=`printf "${data}" | sed 's/\(\s*.*\s*\):\s*.*/\1/g'`
      data_entries+=( "$entry" )
		done;

    secret=`$platform get $line -o yaml -n $namespace`

		for data_key in "${data_entries[@]}"; do
		   secret=`printf "$secret" | sed "s/\s*$data_key\s*:\s*.*/  $data_key: *****/"`
		done;

		printf "$secret" | sed 's/^\(\s*password\s*:\s*\).*\n/\1*****/' \
		| sed 's/^\(\s*.*\.key\s*:\s*\).*/\1*****/' > $direct/reports/$1/"$filename".yaml
	done;

}

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
  if [ "$res" = "secrets" ] && [ "$secrets" = "hidden" ]; then
    get_masked_secrets "secrets"
  else
    if [ ! -z "$res" ]; then
	    get_namespaced_yamls "$res"
    fi
	fi
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
	elif [[ $pod =~ .*-kafka-[0-9]+ ]]; then
	  $platform logs $pod -c tls-sidecar -n $namespace > $direct/reports/podLogs/"$pod"-tls-sidecar.log
	  $platform logs $pod -c kafka -n $namespace > $direct/reports/podLogs/"$pod"-kafka.log
	  $platform logs $pod -p -c tls-sidecar -n $namespace 2>/dev/null > $direct/reports/podLogs/previous-"$pod"-tls-sidecar.log
	  $platform logs $pod -p -c kafka -n $namespace 2>/dev/null > $direct/reports/podLogs/previous-"$pod"-kafka.log
	  $platform exec -i $pod -n $namespace -c kafka -- cat /tmp/strimzi.properties > $direct/reports/configs/"$pod".cfg
	elif [[ $pod =~ .*-zookeeper-[0-9]+ ]]; then
	  $platform logs $pod -c tls-sidecar -n $namespace > $direct/reports/podLogs/"$pod"-tls-sidecar.log	
	  $platform logs $pod -c zookeeper -n $namespace > $direct/reports/podLogs/"$pod"-zookeeper.log
	  $platform logs $pod -p -c tls-sidecar -n $namespace 2>/dev/null > $direct/reports/podLogs/previous-"$pod"-tls-sidecar.log
	  $platform logs $pod -p -c zookeeper -n $namespace 2>/dev/null > $direct/reports/podLogs/previous-"$pod"-zookeeper.log
	  $platform exec -i $pod -n $namespace -c zookeeper -- cat /tmp/zookeeper.properties > $direct/reports/configs/"$pod".cfg
	elif [[ $pod == *"-kafka-exporter-"* || $pod == *"-connect-"* || $pod == *"-bridge-"* || $pod == *"-mirror-maker-"* ]]; then
	  $platform logs $pod -n $namespace > $direct/reports/podLogs/"$pod".log
	  $platform logs $pod -p -n $namespace 2>/dev/null > $direct/reports/podLogs/previous-"$pod".log
	fi
done;

# getting CO deployment from the pod
$platform get deployment strimzi-cluster-operator -o yaml -n $namespace > $direct/reports/deployments/cluster-operator.yaml
$platform get pod -l strimzi.io/kind=cluster-operator -o yaml -n $namespace > $direct/reports/pods/cluster-operator.yaml
co_pod=$($platform get pod -l strimzi.io/kind=cluster-operator -o name -n $namespace)
$platform logs $co_pod -n $namespace > $direct/reports/podLogs/cluster-operator.log
$platform logs $co_pod -p -n $namespace 2>/dev/null > $direct/reports/podLogs/previous-cluster-operator.log

# getting CO replicaset
co_rs=$($platform get replicaset -l strimzi.io/kind=cluster-operator -o name -n $namespace)
$platform describe $co_rs -n $namespace > $direct/reports/replicasets/cluster-operator-replicaset.yaml

#Kafka, KafkaConnect, KafkaTopic, KafkaUser, KafkaMirrorMaker
echo "CRs:"
mkdir -p $direct/reports/crs
crs=$($platform get crd -o name)
for line in $crs; do
  cr=`echo $line | cut -d "/" -f 2`
  if [[ $cr == *"kafka.strimzi.io" ]]; then
    resources=$($platform get $cr -o name -n $namespace | cut -d "/" -f 2)
    if [[ -n "$resources" ]]; then
      echo $cr
      for line in $resources; do
        resource=`echo $line | cut -f 1 -d " "`
        $platform get $cr $resource -n $namespace -o yaml > $direct/reports/crs/"$cr"-"$resource".yaml
        echo "   "$resource
      done;
    fi
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
olddir=$(pwd)
cd /$direct
zip -qr $filename.zip ./reports/
cd $olddir
mv /$direct/$filename.zip ./
echo "Report file $filename.zip created"
