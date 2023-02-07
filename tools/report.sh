#!/usr/bin/env bash
set -Eeuo pipefail
if [[ $(uname -s) == "Darwin" ]]; then
  shopt -s expand_aliases
  alias echo="gecho"; alias grep="ggrep"; alias sed="gsed"; alias date="gdate"
fi

{ # this ensures the entire script is downloaded #
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
SE="s${SD}^\(\s*.*\password\s*:\s*\).*${SD}\1\[hidden\]${SD}; s${SD}^\(\s*.*\.key\s*:\s*\).*${SD}\1\[hidden\]${SD}" && readonly SE

error() {
  echo -n "$@" 1>&2 && exit 1
}

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

# check kube connectivity
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

if [[ $($KUBE_CLIENT get ns "$NAMESPACE" &>/dev/null) == "1" ]]; then
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
    mkdir -p "$OUT_DIR"/reports/podlogs
    if [[ "$count" -eq 1 ]]; then
      logs="$($KUBE_CLIENT -n "$NAMESPACE" logs "$pod" ||true)"
      if [[ -n $logs ]]; then printf "%s" "$logs" > "$OUT_DIR"/reports/podlogs/"$pod".log; fi
      logs="$($KUBE_CLIENT -n "$NAMESPACE" logs "$pod" -p 2>/dev/null ||true)"
      if [[ -n $logs ]]; then printf "%s" "$logs" > "$OUT_DIR"/reports/podlogs/"$pod".log.0; fi
    fi
    if [[ "$count" -gt 1 && -n "$con" && "$names" == *"$con"* ]]; then
      logs="$($KUBE_CLIENT -n "$NAMESPACE" logs "$pod" -c "$con" ||true)"
      if [[ -n $logs ]]; then printf "%s" "$logs" > "$OUT_DIR"/reports/podlogs/"$pod"-"$con".log; fi
      logs="$($KUBE_CLIENT -n "$NAMESPACE" logs "$pod" -p -c "$con" 2>/dev/null ||true)"
      if [[ -n $logs ]]; then printf "%s" "$logs" > "$OUT_DIR"/reports/podlogs/"$pod"-"$con".log.0; fi
    fi
  fi
}

echo "clusteroperator"
CO_DEPLOY=$($KUBE_CLIENT get deploy strimzi-cluster-operator -o name -n "$NAMESPACE" --ignore-not-found) && readonly CO_DEPLOY
if [[ -n $CO_DEPLOY ]]; then
  echo "    $CO_DEPLOY"
  $KUBE_CLIENT get deploy strimzi-cluster-operator -o yaml -n "$NAMESPACE" > "$OUT_DIR"/reports/deployments/cluster-operator.yaml
  $KUBE_CLIENT get po -l strimzi.io/kind=cluster-operator -o yaml -n "$NAMESPACE" > "$OUT_DIR"/reports/pods/cluster-operator.yaml
  CO_POD=$($KUBE_CLIENT get po -l strimzi.io/kind=cluster-operator -o name -n "$NAMESPACE" --ignore-not-found)
  if [[ -n $CO_POD ]]; then
    echo "    $CO_POD"
    CO_POD=$(echo "$CO_POD" | cut -d "/" -f 2) && readonly CO_POD
    get_pod_logs "$CO_POD"
  fi
fi

CO_RS=$($KUBE_CLIENT get rs -l strimzi.io/kind=cluster-operator -o name -n "$NAMESPACE" --ignore-not-found)
if [[ -n $CO_RS ]]; then
  echo "    $CO_RS"
  CO_RS=$(echo "$CO_RS" | tail -n1) && echo "    $CO_RS"
  CO_RS=$(echo "$CO_RS" | cut -d "/" -f 2) && readonly CO_RS
  $KUBE_CLIENT get rs "$CO_RS" -n "$NAMESPACE" > "$OUT_DIR"/reports/replicasets/"$CO_RS".yaml
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
mkdir -p "$OUT_DIR"/reports/crds "$OUT_DIR"/reports/crs
CRDS=$($KUBE_CLIENT get crd -l app=strimzi -o name | cut -d "/" -f 2) && readonly CRDS
for CRD in $CRDS; do
  RES=$($KUBE_CLIENT get "$CRD" -o name -n "$NAMESPACE" | cut -d "/" -f 2)
  if [[ -n $RES ]]; then
    echo "    $CRD"
    $KUBE_CLIENT get crd "$CRD" -o yaml > "$OUT_DIR"/reports/crds/"$CRD".yaml
    for j in $RES; do
      RES=$(echo "$j" | cut -f 1 -d " ")
      $KUBE_CLIENT get "$CRD" "$RES" -n "$NAMESPACE" -o yaml > "$OUT_DIR"/reports/crs/"$CRD"-"$RES".yaml
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

echo "podlogs"
mkdir -p "$OUT_DIR"/reports/configs
PODS=$($KUBE_CLIENT get po -l strimzi.io/cluster="$CLUSTER" -o name -n "$NAMESPACE" | cut -d "/" -f 2)
PODS="$PODS $($KUBE_CLIENT get po -l strimzi.io/cluster="$BRIDGE" -o name -n "$NAMESPACE" | cut -d "/" -f 2)"
PODS="$PODS $($KUBE_CLIENT get po -l strimzi.io/cluster="$CONNECT" -o name -n "$NAMESPACE" | cut -d "/" -f 2)"
PODS="$PODS $($KUBE_CLIENT get po -l strimzi.io/cluster="$MM2" -o name -n "$NAMESPACE" | cut -d "/" -f 2)"
readonly PODS
for POD in $PODS; do
  echo "    $POD"
  if [[ "$POD" =~ .*-zookeeper-[0-9]+ ]]; then
    get_pod_logs "$POD" zookeeper
    get_pod_logs "$POD" tls-sidecar
    $KUBE_CLIENT exec -i "$POD" -n "$NAMESPACE" -c zookeeper -- \
      cat /tmp/zookeeper.properties > "$OUT_DIR"/reports/configs/"$POD".cfg \
      2>/dev/null||true
  elif [[ "$POD" =~ .*-kafka-[0-9]+ ]]; then
    get_pod_logs "$POD" kafka
    get_pod_logs "$POD" tls-sidecar
    $KUBE_CLIENT exec -i "$POD" -n "$NAMESPACE" -c kafka -- \
      cat /tmp/strimzi.properties > "$OUT_DIR"/reports/configs/"$POD".cfg \
      2>/dev/null||true
  elif [[ "$POD" == *"-entity-operator-"* ]]; then
    get_pod_logs "$POD" topic-operator
    get_pod_logs "$POD" user-operator
    get_pod_logs "$POD" tls-sidecar
  elif [[ "$POD" == *"-kafka-exporter-"* ]]; then
    get_pod_logs "$POD"
  elif [[ "$POD" == *"-bridge-"* ]]; then
    get_pod_logs "$POD"
  elif [[ "$POD" == *"-connect-"* ]]; then
    get_pod_logs "$POD"
  elif [[ "$POD" == *"-mirrormaker2-"* ]]; then
    get_pod_logs "$POD"
  elif [[ "$POD" == *"-cruise-control-"* ]]; then
    get_pod_logs "$POD" cruise-control
    get_pod_logs "$POD" tls-sidecar
  fi
done

FILENAME="report-$(date +"%d-%m-%Y_%H-%M-%S")"
OLD_DIR="$(pwd)"
cd "$OUT_DIR" || exit
zip -qr "$FILENAME".zip ./reports/
cd "$OLD_DIR" || exit
if [[ $OUT_DIR == *"tmp."* ]]; then
  # let's keep the old behavior when --out-dir is not specified
  mv "$OUT_DIR"/"$FILENAME".zip ./
fi
echo "Report file $FILENAME.zip created"
} # this ensures the entire script is downloaded #
