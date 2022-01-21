#!/usr/bin/env bash

KUBECTL_INSTALLED=false
OC_INSTALLED=false
KUBE_CLIENT="kubectl"
SECRETS_OPT="hidden"

# sed delimiter
SD=$(echo -en "\001") && readonly SD

error() {
  echo "$@" 1>&2
  exit 1
}

if [[ $(kubectl &>/dev/null) -eq 0 ]]; then
  echo "Using kubectl client"
  KUBECTL_INSTALLED=true
else
  if [[ $(oc &>/dev/null) -eq 0 ]]; then
    echo "Using oc client"
    OC_INSTALLED=true
    KUBE_CLIENT="oc"
  fi
fi

if [[ $OC_INSTALLED = false && $KUBECTL_INSTALLED = false ]]; then
  error "There is no kubectl or oc installed"
fi

readonly USAGE="
Usage: ${0} --namespace=<string> --cluster=<string> --secrets=(off|hidden|all)
Default level of secret verbosity is 'hidden' (only secret key will be reported).
"

OPTSPEC=":-:"
while getopts "$OPTSPEC" optchar; do
  case "${optchar}" in
    -)
      case "${OPTARG}" in
        cluster=*)
          CLUSTER=${OPTARG#*=} && readonly CLUSTER
          ;;
        namespace=*)
          NAMESPACE=${OPTARG#*=} && readonly NAMESPACE
          ;;
        secrets=*)
          SECRETS_OPT=${OPTARG#*=} && readonly SECRETS_OPT
          ;;
        *)
          error "$USAGE"
          ;;
      esac;;
  esac
done
shift $((OPTIND-1))

if [[ -z $CLUSTER && -z $NAMESPACE ]]; then
  echo "--cluster and --namespace are mandatory options."
  error "$USAGE"
fi

if [[ -z $SECRETS_OPT ]]; then
  SECRETS_OPT="hidden"
fi

if [[ "$SECRETS_OPT" != "all" && "$SECRETS_OPT" != "off" && "$SECRETS_OPT" != "hidden" ]]; then
  echo "Unknown secrets verbosity level. Use one of 'off', 'hidden' or 'all'."
  echo " 'all' - secret keys and data values are reported"
  echo " 'hidden' - secrets with only data keys are reported"
  echo " 'off' - secrets are not reported at all"
  echo "Default value is 'hidden'"
  error "$USAGE"
fi

if [[ -z $CLUSTER ]]; then
  echo "Cluster was not specified. Use --cluster option to specify it."
  error "$USAGE"
fi

if [[ -z $NAMESPACE ]]; then
  echo "Namespace was not specified. Use --namespace option to specify it."
  error "$USAGE"
fi

if [[ $($KUBE_CLIENT get ns "$NAMESPACE" &>/dev/null) == "1" ]]; then
  error "Namespace $NAMESPACE not found! Exiting"
fi

if [[ -z $($KUBE_CLIENT get kafka "$CLUSTER" -o name -n "$NAMESPACE" --ignore-not-found) ]]; then
  error "Kafka cluster $CLUSTER in namespace $NAMESPACE not found! Exiting"
fi

TMP="$(mktemp -d)" && readonly TMP
readonly RESOURCES_TO_FETCH=(
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
  "ingresses"
  "routes"
)
readonly NON_NAMESPACED_RESOURCES_TO_FETCH=(
  "clusterroles"
  "clusterrolebindings"
)

if [[ "$SECRETS_OPT" == "off" ]]; then
  RESOURCES_TO_FETCH=("${RESOURCES_TO_FETCH[@]/secrets}")
fi

get_masked_secrets() {
  mkdir -p "$TMP"/reports/secrets
  echo "secrets"
  local resources && resources=$($KUBE_CLIENT get secrets -l strimzi.io/cluster="$CLUSTER" -o name -n "$NAMESPACE")
  for res in $resources; do
    local filename && filename=$(echo "$res" | cut -f 2 -d "/")
    echo "    $res"
    local original_data && original_data=$($KUBE_CLIENT get "$res" -o=jsonpath='{.data}' -n "$NAMESPACE" | cut -c5-)
    SAVEIFS=$IFS
    IFS=$'\n'
    original_data=("$original_data")
    IFS=$SAVEIFS

    local data_entries && data_entries=()
    for data in "${original_data[@]}"; do
      # shellcheck disable=SC2001
      entry=$(echo "${data}" | sed "s${SD}\(\s*.*\s*\):\s*.*${SD}\1${SD}g")
      data_entries+=("$entry")
    done

    local secret && secret=$($KUBE_CLIENT get "$res" -o yaml -n "$NAMESPACE")
    for data_key in "${data_entries[@]}"; do
      secret=$(echo "$secret" | sed "s${SD}\s*$data_key\s*:\s*.*${SD}  $data_key: *****${SD}")
    done
    echo "$secret" | sed "s${SD}^\(\s*password\s*:\s*\).*\n${SD}\1*****${SD}" \
      | sed "s${SD}^\(\s*.*\.key\s*:\s*\).*${SD}\1*****${SD}" > "$TMP"/reports/secrets/"$filename".yaml
  done
}

get_namespaced_yamls() {
  mkdir -p "$TMP"/reports/"$1"
  local resources && resources=$($KUBE_CLIENT get "$1" -l strimzi.io/cluster="$CLUSTER" -o name -n "$NAMESPACE")
  if [[ -n $resources ]]; then
    echo "$1"
    for res in $resources; do
      local filename && filename=$(echo "$res" | cut -f 2 -d "/")
      echo "    $res"
      $KUBE_CLIENT get "$res" -o yaml -n "$NAMESPACE" | sed "s${SD}^\(\s*password\s*:\s*\).*${SD}\1*****${SD}" \
        | sed "s${SD}^\(\s*.*\.key\s*:\s*\).*${SD}\1*****${SD}" > "$TMP"/reports/"$1"/"$filename".yaml
    done
  fi
}

for RES in "${RESOURCES_TO_FETCH[@]}"; do
  if [[ "$RES" == "secrets" && "$SECRETS_OPT" == "hidden" ]]; then
    get_masked_secrets
  else
    if [[ -n "$RES" ]]; then
      get_namespaced_yamls "$RES"
    fi
  fi
done

get_nonnamespaced_yamls() {
  local type="$1"
  mkdir -p "$TMP"/reports/"$type"
  local resources && resources=$($KUBE_CLIENT get "$type" -l app=strimzi -o name -n "$NAMESPACE")
  echo "$type"
  for res in $resources; do
    local resources && resources=$($KUBE_CLIENT get "$type" -l app=strimzi -o name -n "$NAMESPACE")
    echo "    $res"
    res=$(echo "$res" | cut -d "/" -f 2)
    $KUBE_CLIENT get "$type" "$res" -o yaml | sed "s${SD}^\(\s*password\s*:\s*\).*${SD}\1*****${SD}" \
      | sed "s${SD}^\(\s*.*\.key\s*:\s*\).*${SD}\1*****${SD}" > "$TMP"/reports/"$type"/"$res".yaml
  done
}

for RES in "${NON_NAMESPACED_RESOURCES_TO_FETCH[@]}"; do
  get_nonnamespaced_yamls "$RES"
done

get_container_logs_if_present() {
  local pod="$1"
  local con="$2"
  local out="$3"
  if [[ -n $pod && -n $con && -n $out ]]; then
    local found && found="$($KUBE_CLIENT -n "$NAMESPACE" exec "$pod" -c "$con" -- echo true 2>/dev/null ||true)"
    if [[ $found == true ]]; then
      local logs && logs="$($KUBE_CLIENT -n "$NAMESPACE" logs "$pod" -c "$con")"
      if [[ -n $logs ]]; then echo "$logs" > "$out"; fi
      logs="$($KUBE_CLIENT -n "$NAMESPACE" logs "$pod" -p -c "$con" 2>/dev/null ||true)"
      if [[ -n $logs ]]; then echo "$logs" > "$out".0; fi
    fi
  fi
}

echo "podlogs"
mkdir -p "$TMP"/reports/podlogs
mkdir -p "$TMP"/reports/configs
PODS=$($KUBE_CLIENT get pods -l strimzi.io/cluster="$CLUSTER" -o name -n "$NAMESPACE" | cut -d "/" -f 2) && readonly PODS
for POD in $PODS; do
  echo "    $POD"
  if [[ "$POD" == *"-entity-operator-"* ]]; then
    get_container_logs_if_present "$POD" topic-operator "$TMP"/reports/podlogs/"$POD"-topic-operator.log
    get_container_logs_if_present "$POD" user-operator "$TMP"/reports/podlogs/"$POD"-user-operator.log
    get_container_logs_if_present "$POD" tls-sidecar "$TMP"/reports/podlogs/"$POD"-tls-sidecar.log
  elif [[ "$POD" =~ .*-kafka-[0-9]+ ]]; then
    get_container_logs_if_present "$POD" kafka "$TMP"/reports/podlogs/"$POD".log
    get_container_logs_if_present "$POD" tls-sidecar "$TMP"/reports/podlogs/"$POD"-tls-sidecar.log
    $KUBE_CLIENT exec -i "$POD" -n "$NAMESPACE" -c kafka -- cat /tmp/strimzi.properties > "$TMP"/reports/configs/"$POD".cfg
  elif [[ "$POD" =~ .*-zookeeper-[0-9]+ ]]; then
    get_container_logs_if_present "$POD" zookeeper "$TMP"/reports/podlogs/"$POD".log
    get_container_logs_if_present "$POD" tls-sidecar "$TMP"/reports/podlogs/"$POD"-tls-sidecar.log
    $KUBE_CLIENT exec -i "$POD" -n "$NAMESPACE" -c zookeeper -- cat /tmp/zookeeper.properties > "$TMP"/reports/configs/"$POD".cfg
  elif [[ "$POD" == *"-kafka-exporter-"* || "$POD" == *"-connect-"* || "$POD" == *"-bridge-"* || "$POD" == *"-mirror-maker-"* ]]; then
    $KUBE_CLIENT logs "$POD" -n "$NAMESPACE" > "$TMP"/reports/podlogs/"$POD".log
    PPLOG=$($KUBE_CLIENT logs "$POD" -p -n "$NAMESPACE" 2>/dev/null ||true)
    if [[ -n $PPLOG ]]; then echo "$PPLOG" > "$TMP"/reports/podlogs/"$POD".log.0; fi
  fi
done

CO_DEPLOY=$($KUBE_CLIENT get deploy strimzi-cluster-operator -o name -n "$NAMESPACE" --ignore-not-found) && readonly CO_DEPLOY
if [[ -n $CO_DEPLOY ]]; then
  echo "clusteroperator"
  echo "    $CO_DEPLOY"
  $KUBE_CLIENT get deploy strimzi-cluster-operator -o yaml -n "$NAMESPACE" > "$TMP"/reports/deployments/cluster-operator.yaml
  $KUBE_CLIENT get po -l strimzi.io/kind=cluster-operator -o yaml -n "$NAMESPACE" > "$TMP"/reports/pods/cluster-operator.yaml
  CO_POD=$($KUBE_CLIENT get po -l strimzi.io/kind=cluster-operator -o name -n "$NAMESPACE" --ignore-not-found)
  if [[ -n $CO_POD ]]; then
    echo "    $CO_POD"
    CO_POD=$(echo "$CO_POD" | cut -d "/" -f 2) && readonly CO_POD
    $KUBE_CLIENT logs "$CO_POD" -n "$NAMESPACE" > "$TMP"/reports/podlogs/cluster-operator.log
    $KUBE_CLIENT logs "$CO_POD" -p -n "$NAMESPACE" 2>/dev/null > "$TMP"/reports/podlogs/cluster-operator.log.0
  fi
fi

CO_RS=$($KUBE_CLIENT get rs -l strimzi.io/kind=cluster-operator -o name -n "$NAMESPACE" --ignore-not-found)
if [[ -n $CO_RS ]]; then
  CO_RS=$(echo "$CO_RS" | tail -n1) && echo "    $CO_RS"
  CO_RS=$(echo "$CO_RS" | cut -d "/" -f 2) && readonly CO_RS
  $KUBE_CLIENT get rs "$CO_RS" -n "$NAMESPACE" > "$TMP"/reports/replicasets/"$CO_RS".yaml
fi

echo "customresources"
mkdir -p "$TMP"/reports/crs
CRS=$($KUBE_CLIENT get crd -o name | cut -d "/" -f 2) && readonly CRS
for CR in $CRS; do
  if [[ $CR == *".strimzi.io" ]]; then
    RES=$($KUBE_CLIENT get "$CR" -o name -n "$NAMESPACE" | cut -d "/" -f 2)
    if [[ -n $RES ]]; then
      echo "    $CR"
      for j in $RES; do
        RES=$(echo "$j" | cut -f 1 -d " ")
        $KUBE_CLIENT get "$CR" "$RES" -n "$NAMESPACE" -o yaml > "$TMP"/reports/crs/"$CR"-"$RES".yaml
        echo "        $RES"
      done
    fi
  fi
done

echo "customresourcedefinitions"
mkdir -p "$TMP"/reports/crds
CRDS=$($KUBE_CLIENT get crd -o name) && readonly CRDS
for CRD in $CRDS; do
  CRD=$(echo "$CRD" | cut -d "/" -f 2)
  if [[ "$CRD" == *".strimzi.io" ]]; then
    echo "    $CRD"
    $KUBE_CLIENT get crd "$CRD" -o yaml > "$TMP"/reports/crds/"$CRD".yaml
  fi
done

mkdir -p "$TMP"/reports/events
EVENTS=$($KUBE_CLIENT get event -n "$NAMESPACE" --ignore-not-found) && readonly EVENTS
if [[ -n $EVENTS ]]; then
  echo "Events found"
  echo "$EVENTS" > "$TMP"/reports/events/events.txt
else
  echo "No event found"
fi

FILENAME="report-$(date +"%d-%m-%Y_%H-%M-%S")" && readonly FILENAME
OLD_DIR="$(pwd)" && readonly OLD_DIR
cd "$TMP" || exit
zip -qr "$FILENAME".zip ./reports/
cd "$OLD_DIR" || exit
mv "$TMP"/"$FILENAME".zip ./
echo "Report file $FILENAME.zip created"
