#!/usr/bin/env bash
# Self contained Strimzi reporting tool.
set -Eeuo pipefail
if [[ $(uname -s) == "Darwin" ]]; then
  shopt -s expand_aliases
  alias echo="gecho"; alias grep="ggrep"; alias sed="gsed"; alias date="gdate"
fi

{ # this ensures that the entire script is downloaded #
NAMESPACE=""
CLUSTER=""
BRIDGE=""
CONNECT=""
MM2=""
KUBECTL_INSTALLED=false
OC_INSTALLED=false
KUBE_CLIENT="kubectl"
OUT_DIR=""
SECRETS_OPT="hidden"

# sed non-printable text delimiter
SD=$(echo -en "\001") && readonly SD
# sed sensitive information filter expression
SE="s${SD}^\(\s*.*\password\s*:\s*\).*${SD}\1\<hidden>'${SD}; s${SD}^\(\s*.*\.key\s*:\s*\).*${SD}\1\<hidden>${SD}" && readonly SE

error() {
  echo "$@" 1>&2 && exit 1
}

# bash version check
if [[ -z ${BASH_VERSINFO+x} ]]; then
  error "No bash version information available, aborting"
fi
if [[ "${BASH_VERSINFO[0]}" -lt 4 ]]; then
  error "You need bash version >= 4 to run the script"
fi

# kube client check
if [[ -x "$(command -v kubectl)" ]]; then
  KUBECTL_INSTALLED=true
else
  if [[ -x "$(command -v oc)" ]]; then
    OC_INSTALLED=true
    KUBE_CLIENT="oc"
  fi
fi
if [[ $OC_INSTALLED = false && $KUBECTL_INSTALLED = false ]]; then
  error "There is no kubectl or oc installed"
fi

# kube connectivity check
$KUBE_CLIENT version -o yaml --request-timeout=5s 1>/dev/null

readonly USAGE="
Usage: report.sh [options]

Required:
  --namespace=<string>          Kubernetes namespace.
  --cluster=<string>            Kafka cluster name.

Optional:
  --bridge=<string>             Bridge component name to get pods and logs.
  --connect=<string>            Connect component name to get pods and logs.
  --mm2=<string>                MM2 component name to get pods and logs.
  --secrets=(off|hidden|all)    Secret verbosity. Default is hidden, only the secret key will be reported.
  --out-dir=<string>            Script output directory.
"
OPTSPEC=":-:"
while getopts "$OPTSPEC" optchar; do
  case "${optchar}" in
    -)
      case "${OPTARG}" in
        namespace=*)
          NAMESPACE=${OPTARG#*=} && readonly NAMESPACE
          ;;
        out-dir=*)
          OUT_DIR=${OPTARG#*=}
          OUT_DIR=${OUT_DIR//\~/$HOME} && readonly OUT_DIR
          ;;
        cluster=*)
          CLUSTER=${OPTARG#*=} && readonly CLUSTER
          ;;
        bridge=*)
          BRIDGE=${OPTARG#*=} && readonly BRIDGE
          ;;
        connect=*)
          CONNECT=${OPTARG#*=} && readonly CONNECT
          ;;
        mm2=*)
          MM2=${OPTARG#*=} && readonly MM2
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

if [[ -z $NAMESPACE || -z $CLUSTER ]]; then
  error "$USAGE"
fi

if [[ -z $OUT_DIR ]]; then
  OUT_DIR="$(mktemp -d)"
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

if [[ -z $($KUBE_CLIENT get ns "$NAMESPACE" -o name --ignore-not-found) ]]; then
  error "Namespace $NAMESPACE not found! Exiting"
fi

if [[ -z $($KUBE_CLIENT get kafkas.kafka.strimzi.io "$CLUSTER" -o name -n "$NAMESPACE" --ignore-not-found) ]]; then
  error "Kafka cluster $CLUSTER in namespace $NAMESPACE not found! Exiting"
fi

RESOURCES=(
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
readonly CLUSTER_RESOURCES=(
  "clusterroles"
  "clusterrolebindings"
)

if [[ "$SECRETS_OPT" == "off" ]]; then
  RESOURCES=("${RESOURCES[@]/secrets}") && readonly RESOURCES
fi

get_masked_secrets() {
  echo "secrets"
  mkdir -p "$OUT_DIR"/reports/secrets
  local resources && resources=$($KUBE_CLIENT get secrets -l strimzi.io/cluster="$CLUSTER" -o name -n "$NAMESPACE")
  for res in $resources; do
    local filename && filename=$(echo "$res" | cut -f 2 -d "/")
    echo "    $res"
    local secret && secret=$($KUBE_CLIENT get "$res" -o yaml -n "$NAMESPACE")
    if [[ "$SECRETS_OPT" == "all" ]]; then
      echo "$secret" > "$OUT_DIR"/reports/secrets/"$filename".yaml
    else
      echo "$secret" | sed "$SE" > "$OUT_DIR"/reports/secrets/"$filename".yaml
    fi
  done
}

get_namespaced_yamls() {
  local type="$1"
  mkdir -p "$OUT_DIR"/reports/"$type"
  local resources
  resources=$($KUBE_CLIENT get "$type" -l strimzi.io/cluster="$CLUSTER" -o name -n "$NAMESPACE" 2>/dev/null ||true)
  resources="$resources $($KUBE_CLIENT get "$type" -l strimzi.io/cluster="$BRIDGE" -o name -n "$NAMESPACE" 2>/dev/null ||true)"
  resources="$resources $($KUBE_CLIENT get "$type" -l strimzi.io/cluster="$CONNECT" -o name -n "$NAMESPACE" 2>/dev/null ||true)"
  resources="$resources $($KUBE_CLIENT get "$type" -l strimzi.io/cluster="$MM2" -o name -n "$NAMESPACE" 2>/dev/null ||true)"
  echo "$type"
  if [[ -n $resources ]]; then
    for res in $resources; do
      local filename && filename=$(echo "$res" | cut -f 2 -d "/")
      echo "    $res"
      if [[ "$SECRETS_OPT" == "all" ]]; then
        $KUBE_CLIENT get "$res" -o yaml -n "$NAMESPACE" > "$OUT_DIR"/reports/"$type"/"$filename".yaml
      else
        $KUBE_CLIENT get "$res" -o yaml -n "$NAMESPACE" | sed "$SE" > "$OUT_DIR"/reports/"$type"/"$filename".yaml
      fi
    done
  fi
}

for RES in "${RESOURCES[@]}"; do
  if [[ -n "$RES" ]]; then
    get_namespaced_yamls "$RES"
  fi
done

get_nonnamespaced_yamls() {
  local type="$1"
  mkdir -p "$OUT_DIR"/reports/"$type"
  local resources && resources=$($KUBE_CLIENT get "$type" -l app=strimzi -o name -n "$NAMESPACE")
  echo "$type"
  for res in $resources; do
    local resources && resources=$($KUBE_CLIENT get "$type" -l app=strimzi -o name -n "$NAMESPACE")
    echo "    $res"
    res=$(echo "$res" | cut -d "/" -f 2)
    $KUBE_CLIENT get "$type" "$res" -o yaml | sed "$SE" > "$OUT_DIR"/reports/"$type"/"$res".yaml
  done
}

for RES in "${CLUSTER_RESOURCES[@]}"; do
  get_nonnamespaced_yamls "$RES"
done

get_pod_logs() {
  local pod="$1"
  local con="${2-}"
  if [[ -n $pod ]]; then
    local names && names=$($KUBE_CLIENT -n "$NAMESPACE" get po "$pod" -o jsonpath='{.spec.containers[*].name}' --ignore-not-found)
    local count && count=$(echo "$names" | wc -w)
    local logs
    mkdir -p "$OUT_DIR"/reports/logs
    if [[ "$count" -eq 1 ]]; then
      logs="$($KUBE_CLIENT -n "$NAMESPACE" logs "$pod" ||true)"
      if [[ -n $logs ]]; then printf "%s" "$logs" > "$OUT_DIR"/reports/logs/"$pod".log; fi
      logs="$($KUBE_CLIENT -n "$NAMESPACE" logs "$pod" -p 2>/dev/null ||true)"
      if [[ -n $logs ]]; then printf "%s" "$logs" > "$OUT_DIR"/reports/logs/"$pod".log.0; fi
    fi
    if [[ "$count" -gt 1 && -n "$con" && "$names" == *"$con"* ]]; then
      logs="$($KUBE_CLIENT -n "$NAMESPACE" logs "$pod" -c "$con" ||true)"
      if [[ -n $logs ]]; then printf "%s" "$logs" > "$OUT_DIR"/reports/logs/"$pod"-"$con".log; fi
      logs="$($KUBE_CLIENT -n "$NAMESPACE" logs "$pod" -p -c "$con" 2>/dev/null ||true)"
      if [[ -n $logs ]]; then printf "%s" "$logs" > "$OUT_DIR"/reports/logs/"$pod"-"$con".log.0; fi
    fi
  fi
}

echo "clusteroperator"
CO_DEPLOY=$($KUBE_CLIENT get deploy strimzi-cluster-operator -o name -n "$NAMESPACE" --ignore-not-found)
if [[ -n $CO_DEPLOY ]]; then
  echo "    $CO_DEPLOY"
  CO_DEPLOY=$(echo "$CO_DEPLOY" | cut -d "/" -f 2) && readonly CO_DEPLOY
  $KUBE_CLIENT get deploy "$CO_DEPLOY" -o yaml -n "$NAMESPACE" > "$OUT_DIR"/reports/deployments/"$CO_DEPLOY".yaml
  CO_RS=$($KUBE_CLIENT get rs -l strimzi.io/kind=cluster-operator -o name -n "$NAMESPACE" --ignore-not-found)
  if [[ -n $CO_RS ]]; then
    echo "    $CO_RS"
    CO_RS=$(echo "$CO_RS" | cut -d "/" -f 2) && readonly CO_RS
    for res in $CO_RS; do
      $KUBE_CLIENT get rs "$res" -o yaml -n "$NAMESPACE" > "$OUT_DIR"/reports/replicasets/"$res".yaml
    done
  fi
  mapfile -t CO_PODS < <($KUBE_CLIENT get po -l strimzi.io/kind=cluster-operator -o name -n "$NAMESPACE" --ignore-not-found)
  if [[ ${#CO_PODS[@]} -ne 0 ]]; then
    for pod in "${CO_PODS[@]}"; do
      echo "    $pod"
      CO_POD=$(echo "$pod" | cut -d "/" -f 2)
      $KUBE_CLIENT get po "$CO_POD" -o yaml -n "$NAMESPACE" > "$OUT_DIR"/reports/pods/"$CO_POD".yaml
      get_pod_logs "$CO_POD"
    done
  fi
  CO_CM=$($KUBE_CLIENT get cm strimzi-cluster-operator -o name -n "$NAMESPACE" --ignore-not-found)
  if [[ -n $CO_CM ]]; then
    echo "    $CO_CM"
    CO_CM=$(echo "$CO_CM" | cut -d "/" -f 2) && readonly CO_CM
    $KUBE_CLIENT get cm "$CO_CM" -o yaml -n "$NAMESPACE" > "$OUT_DIR"/reports/configmaps/"$CO_CM".yaml
  fi
fi

echo "draincleaner"
DC_DEPLOY=$($KUBE_CLIENT get deploy strimzi-drain-cleaner -o name -n "$NAMESPACE" --ignore-not-found) && readonly DC_DEPLOY
if [[ -n $DC_DEPLOY ]]; then
  echo "    $DC_DEPLOY"
  $KUBE_CLIENT get deploy strimzi-drain-cleaner -o yaml -n "$NAMESPACE" > "$OUT_DIR"/reports/deployments/drain-cleaner.yaml
  $KUBE_CLIENT get po -l app=strimzi-drain-cleaner -o yaml -n "$NAMESPACE" > "$OUT_DIR"/reports/pods/drain-cleaner.yaml
  DC_POD=$($KUBE_CLIENT get po -l app=strimzi-drain-cleaner -o name -n "$NAMESPACE" --ignore-not-found)
  if [[ -n $DC_POD ]]; then
    echo "    $DC_POD"
    DC_POD=$(echo "$DC_POD" | cut -d "/" -f 2) && readonly DC_POD
    get_pod_logs "$DC_POD"
  fi
fi

echo "customresources"
mkdir -p "$OUT_DIR"/reports/customresourcedefinitions
CRDS=$($KUBE_CLIENT get crd -l app=strimzi --ignore-not-found --no-headers -o=custom-columns=NAME:.metadata.name) && readonly CRDS
for CRD in $CRDS; do
  RES=$($KUBE_CLIENT get "$CRD" -o name -n "$NAMESPACE" | cut -d "/" -f 2)
  if [[ -n $RES ]]; then
    echo "    $CRD"
    $KUBE_CLIENT get crd "$CRD" -o yaml > "$OUT_DIR"/reports/customresourcedefinitions/"$CRD".yaml
    CR_DIR="${CRD//.*/}" && mkdir -p "$OUT_DIR"/reports/"$CR_DIR"
    for j in $RES; do
      RES=$(echo "$j" | cut -f 1 -d " ")
      $KUBE_CLIENT get "$CRD" "$RES" -n "$NAMESPACE" -o yaml > "$OUT_DIR"/reports/"$CR_DIR"/"$RES".yaml
      echo "        $RES"
    done
  fi
done

echo "events"
EVENTS=$($KUBE_CLIENT get event -n "$NAMESPACE" --ignore-not-found) && readonly EVENTS
if [[ -n $EVENTS ]]; then
  mkdir -p "$OUT_DIR"/reports/events
  echo "$EVENTS" > "$OUT_DIR"/reports/events/events.txt
fi

echo "logs"
mkdir -p "$OUT_DIR"/reports/configs
mapfile -t KAFKA_PODS < <($KUBE_CLIENT -n "$NAMESPACE" get po -l strimzi.io/kind=Kafka,strimzi.io/name="$CLUSTER-kafka" --ignore-not-found --no-headers -o=custom-columns=NAME:.metadata.name)
if [[ ${#KAFKA_PODS[@]} -ne 0 ]]; then
  for pod in "${KAFKA_PODS[@]}"; do
    echo "    $pod"
    get_pod_logs "$pod" kafka
    get_pod_logs "$pod" tls-sidecar # for old Strimzi releases
    $KUBE_CLIENT -n "$NAMESPACE" exec -i "$pod" -c kafka -- \
      cat /tmp/strimzi.properties > "$OUT_DIR"/reports/configs/"$pod".cfg 2>/dev/null||true
  done
fi
mapfile -t ZOO_PODS < <($KUBE_CLIENT -n "$NAMESPACE" get po -l strimzi.io/kind=Kafka,strimzi.io/name="$CLUSTER-zookeeper" --ignore-not-found --no-headers -o=custom-columns=NAME:.metadata.name)
if [[ ${#ZOO_PODS[@]} -ne 0 ]]; then
  for pod in "${ZOO_PODS[@]}"; do
    echo "    $pod"
    get_pod_logs "$pod" zookeeper
    get_pod_logs "$pod" tls-sidecar # for old Strimzi releases
    $KUBE_CLIENT exec -i "$pod" -n "$NAMESPACE" -c zookeeper -- \
      cat /tmp/zookeeper.properties > "$OUT_DIR"/reports/configs/"$pod".cfg 2>/dev/null||true
  done
fi
mapfile -t ENTITY_PODS < <($KUBE_CLIENT -n "$NAMESPACE" get po -l strimzi.io/kind=Kafka,strimzi.io/name="$CLUSTER-entity-operator" --ignore-not-found --no-headers -o=custom-columns=NAME:.metadata.name)
if [[ ${#ENTITY_PODS[@]} -ne 0 ]]; then
  for pod in "${ENTITY_PODS[@]}"; do
    echo "    $pod"
    get_pod_logs "$pod" topic-operator
    get_pod_logs "$pod" user-operator
    get_pod_logs "$pod" tls-sidecar
  done
fi
mapfile -t CC_PODS < <($KUBE_CLIENT -n "$NAMESPACE" get po -l strimzi.io/kind=Kafka,strimzi.io/name="$CLUSTER-cruise-control" --ignore-not-found --no-headers -o=custom-columns=NAME:.metadata.name)
if [[ ${#CC_PODS[@]} -ne 0 ]]; then
  for pod in "${CC_PODS[@]}"; do
    echo "    $pod"
    get_pod_logs "$pod"
    get_pod_logs "$pod" tls-sidecar # for old Strimzi releases
  done
fi
mapfile -t EXPORTER_PODS < <($KUBE_CLIENT -n "$NAMESPACE" get po -l strimzi.io/kind=Kafka,strimzi.io/name="$CLUSTER-kafka-exporter" --ignore-not-found --no-headers -o=custom-columns=NAME:.metadata.name)
if [[ ${#EXPORTER_PODS[@]} -ne 0 ]]; then
  for pod in "${EXPORTER_PODS[@]}"; do
    echo "    $pod"
    get_pod_logs "$pod"
  done
fi
if [[ -n $BRIDGE ]]; then
  mapfile -t BRIDGE_PODS < <($KUBE_CLIENT -n "$NAMESPACE" get po -l strimzi.io/kind=KafkaBridge,strimzi.io/name="$BRIDGE-bridge" --ignore-not-found --no-headers -o=custom-columns=NAME:.metadata.name)
  if [[ ${#BRIDGE_PODS[@]} -ne 0 ]]; then
    for pod in "${BRIDGE_PODS[@]}"; do
      echo "    $pod"
      get_pod_logs "$pod"
    done
  fi
fi
if [[ -n $CONNECT ]]; then
  mapfile -t CONNECT_PODS < <($KUBE_CLIENT -n "$NAMESPACE" get po -l strimzi.io/kind=KafkaConnect,strimzi.io/name="$CONNECT-connect" --ignore-not-found --no-headers -o=custom-columns=NAME:.metadata.name)
  if [[ ${#CONNECT_PODS[@]} -ne 0 ]]; then
    for pod in "${CONNECT_PODS[@]}"; do
      echo "    $pod"
      get_pod_logs "$pod"
    done
  fi
fi
if [[ -n $MM2 ]]; then
  mapfile -t MM2_PODS < <($KUBE_CLIENT -n "$NAMESPACE" get po -l strimzi.io/kind=KafkaMirrorMaker2,strimzi.io/name="$MM2-mirrormaker2" --ignore-not-found --no-headers -o=custom-columns=NAME:.metadata.name)
  if [[ ${#MM2_PODS[@]} -ne 0 ]]; then
    for pod in "${MM2_PODS[@]}"; do
      echo "    $pod"
      get_pod_logs "$pod"
    done
  fi
fi

FILENAME="report-$(date +"%d-%m-%Y_%H-%M-%S")"
OLD_DIR="$(pwd)"
cd "$OUT_DIR" || exit
zip -qr "$FILENAME".zip ./reports/
cd "$OLD_DIR" || exit
if [[ $OUT_DIR == *"tmp."* ]]; then
  # keeping the old behavior when --out-dir is not specified
  mv "$OUT_DIR"/"$FILENAME".zip ./
fi
echo "Report file $FILENAME.zip created"
} # this ensures that the entire script is downloaded #
