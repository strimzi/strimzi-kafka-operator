#!/usr/bin/env bash

oc_installed=false
kubectl_installed=false
platform="kubectl"
secrets="hidden"

# sed delimiter
readonly sd=$(echo -en "\001")

kubectl &>/dev/null
if [ $? -eq 0 ]; then
	kubectl_installed=true
else
  oc &>/dev/null
  if [ $? -eq 0 ]; then
  	oc_installed=true
  	platform="oc"
  fi
fi

if [[ $oc_installed = false && $kubectl_installed = false ]]; then
	echo "There is no oc or kubectl installed"
	exit 1
fi

usage() {
	echo "Usage: $0 --namespace=<string> --cluster=<string> --secrets=(off|hidden|all)" 1>&2
	echo "Default level of secret verbosity is 'hidden' (only secret key will be reported)."
	exit 1
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

direct=$(mktemp -d)
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
	"pods"
	"persistentvolumeclaims"
	"routes"
	)

if [ "$secrets" = "off" ]; then
  resources_to_fetch=("${resources_to_fetch[@]/secrets}")
fi

get_masked_secrets() {
	mkdir -p $direct/reports/$1
	echo "$1"
	resources=$($platform get $1 -l strimzi.io/cluster=$cluster -o name -n $namespace)
	for line in $resources; do
		filename=$(echo $line | cut -f 2 -d "/")
		echo "   "$line
		original_data=$($platform get $line -o=jsonpath='{.data}' -n $namespace | cut -c5-)
		SAVEIFS=$IFS
    IFS=$'\n'
    original_data=($original_data)
    IFS=$SAVEIFS

		data_entries=()
		for data in $original_data; do
		  entry=$(echo "${data}" | sed "s${sd}\(\s*.*\s*\):\s*.*${sd}\1${sd}g")
      data_entries+=( "$entry" )
		done

    secret=$($platform get $line -o yaml -n $namespace)

		for data_key in "${data_entries[@]}"; do
		   secret=$(echo "$secret" | sed "s${sd}\s*$data_key\s*:\s*.*${sd}  $data_key: *****${sd}")
		done

		echo "$secret" | sed "s${sd}^\(\s*password\s*:\s*\).*\n${sd}\1*****${sd}" \
		| sed "s${sd}^\(\s*.*\.key\s*:\s*\).*${sd}\1*****${sd}" > $direct/reports/$1/"$filename".yaml
	done
}

nonnamespaced_resources_to_fetch=(
	"clusterroles"
	"clusterrolebindings"
	)

get_namespaced_yamls() {
	mkdir -p $direct/reports/$1
	resources=$($platform get $1 -l strimzi.io/cluster=$cluster -o name -n $namespace)
	if [[ -n $resources ]]; then
    echo "$1"
    for line in $resources; do
      filename=$(echo $line | cut -f 2 -d "/")
      echo "   "$line
      $platform get $line -o yaml -n $namespace | sed "s${sd}^\(\s*password\s*:\s*\).*${sd}\1*****${sd}" \
      | sed "s${sd}^\(\s*.*\.key\s*:\s*\).*${sd}\1*****${sd}" > $direct/reports/"$1"/"$filename".yaml
    done
  fi
}

for res in "${resources_to_fetch[@]}"; do
  if [ "$res" = "secrets" ] && [ "$secrets" = "hidden" ]; then
    get_masked_secrets "secrets"
  else
    if [ ! -z "$res" ]; then
	    get_namespaced_yamls "$res"
    fi
	fi
done

get_container_logs_if_present() {
  local pod="$1"
  local con="$2"
  local out="$3"
  if [[ -n $pod && -n $con && -n $out ]]; then
    local found="$($platform -n $namespace exec $pod -c $con -- echo true 2>/dev/null ||true)"
    if [[ $found == true ]]; then
      local logs="$($platform -n $namespace logs $pod -c $con)"
      if [[ -n $logs ]]; then echo "$logs" > $out; fi
      logs="$($platform -n $namespace logs $pod -p -c $con 2>/dev/null ||true)"
      if [[ -n $logs ]]; then echo "$logs" > $out.0; fi
    fi
  fi
}

get_nonnamespaced_yamls() {
	mkdir -p $direct/reports/$1
	resources=$($platform get $1 -l app=strimzi -o name -n $namespace)
	echo "$1"
	for line in $resources; do
		filename=$(echo $line | cut -f 2 -d "/")
		echo "   "$line
		$platform get $line -o yaml | sed "s${sd}^\(\s*password\s*:\s*\).*${sd}\1*****${sd}" \
		| sed "s${sd}^\(\s*.*\.key\s*:\s*\).*${sd}\1*****${sd}" > $direct/reports/"$1"/"$filename".yaml
	done
}

for res in "${nonnamespaced_resources_to_fetch[@]}"; do
	get_nonnamespaced_yamls "$res"
done

mkdir -p $direct/reports/podlogs
mkdir -p $direct/reports/configs

echo "podlogs"
pods=$($platform get pods -l strimzi.io/cluster=$cluster -o name -n $namespace | cut -d "/" -f 2)
for pod in $pods; do
	echo "   "$pod
	if [[ $pod == *"-entity-operator-"* ]]; then
	  get_container_logs_if_present $pod topic-operator $direct/reports/podlogs/$pod-topic-operator.log
	  get_container_logs_if_present $pod user-operator $direct/reports/podlogs/$pod-user-operator.log
	  get_container_logs_if_present $pod tls-sidecar $direct/reports/podlogs/$pod-tls-sidecar.log
	elif [[ $pod =~ .*-kafka-[0-9]+ ]]; then
	  get_container_logs_if_present $pod kafka $direct/reports/podlogs/$pod.log
	  get_container_logs_if_present $pod tls-sidecar $direct/reports/podlogs/$pod-tls-sidecar.log
	  $platform exec -i $pod -n $namespace -c kafka -- cat /tmp/strimzi.properties > $direct/reports/configs/$pod.cfg
	elif [[ $pod =~ .*-zookeeper-[0-9]+ ]]; then
	  get_container_logs_if_present $pod zookeeper $direct/reports/podlogs/$pod.log
	  get_container_logs_if_present $pod tls-sidecar $direct/reports/podlogs/$pod-tls-sidecar.log
	  $platform exec -i $pod -n $namespace -c zookeeper -- cat /tmp/zookeeper.properties > $direct/reports/configs/$pod.cfg
	elif [[ $pod == *"-kafka-exporter-"* || $pod == *"-connect-"* || $pod == *"-bridge-"* || $pod == *"-mirror-maker-"* ]]; then
	  $platform logs $pod -n $namespace > $direct/reports/podlogs/$pod.log
	  $platform logs $pod -p -n $namespace 2>/dev/null > $direct/reports/podlogs/$pod.log.0
	fi
done

co_deploy=$($platform get deploy strimzi-cluster-operator -o name -n $namespace --ignore-not-found)
if [[ -n $co_deploy ]]; then
  $platform get deploy strimzi-cluster-operator -o yaml -n $namespace > $direct/reports/deployments/cluster-operator.yaml
  $platform get po -l strimzi.io/kind=cluster-operator -o yaml -n $namespace > $direct/reports/pods/cluster-operator.yaml
  co_pod=$($platform get po -l strimzi.io/kind=cluster-operator -o name -n $namespace --ignore-not-found)
  if [[ -n $co_pod ]]; then
    co_pod=$(echo "$co_pod" | cut -d "/" -f 2)
    $platform logs $co_pod -n $namespace > $direct/reports/podlogs/cluster-operator.log
    $platform logs $co_pod -p -n $namespace 2>/dev/null > $direct/reports/podlogs/cluster-operator.log.0
  fi
fi

co_rs=$($platform get replicaset -l strimzi.io/kind=cluster-operator -o name -n $namespace --ignore-not-found)
if [[ -n $co_rs ]]; then
  $platform describe $co_rs -n $namespace > $direct/reports/replicasets/cluster-operator-replicaset.yaml
fi

echo "customresources"
mkdir -p $direct/reports/crs
crs=$($platform get crd -o name)
for line in $crs; do
  cr=$(echo $line | cut -d "/" -f 2)
  if [[ $cr == *".strimzi.io" ]]; then
    resources=$($platform get $cr -o name -n $namespace | cut -d "/" -f 2)
    if [[ -n $resources ]]; then
      echo "    "$cr
      for line in $resources; do
        resource=$(echo $line | cut -f 1 -d " ")
        $platform get $cr $resource -n $namespace -o yaml > $direct/reports/crs/"$cr"-"$resource".yaml
        echo "       "$resource
      done
    fi
  fi
done

echo "customresourcedefinitions"
mkdir -p $direct/reports/crds
crds=$($platform get crd -o name)
for line in $crds; do
	crd=$(echo $line | cut -d "/" -f 2)
	if [[ $crd == *".strimzi.io" ]]; then
		echo "   "$crd
		$platform get crd $crd -o yaml > $direct/reports/crds/"$crd".yaml
	fi
done

events=$($platform get event -n $namespace --ignore-not-found)
if [[ -n $events ]]; then
  mkdir -p $direct/reports/events
  echo "$events" > $direct/reports/events/events.yaml
fi

filename=$(date +"%d-%m-%Y_%H-%M-%S")
filename=report-"$filename"
olddir=$(pwd)
cd $direct
zip -qr $filename.zip ./reports/
cd $olddir
mv $direct/$filename.zip ./
echo "Report file $filename.zip created"
