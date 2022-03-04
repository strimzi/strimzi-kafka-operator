#!/usr/bin/env bash
set -Eeuo pipefail
if [[ $(uname -s) == "Darwin" ]]; then
  shopt -s expand_aliases
  alias echo="gecho"; alias dirname="gdirname"; alias grep="ggrep"; alias readlink="greadlink"
  alias tar="gtar"; alias sed="gsed"; alias date="gdate"; alias ls="gls"
fi
BASE="" && pushd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" >/dev/null \
  && { BASE=$PWD; popd >/dev/null; } && readonly BASE
trap 'failure "$LINENO" "$BASH_COMMAND"' ERR

readonly RSYNC_POD_NAME="cold-backup"
ZK_REPLICAS=0
ZK_PVC=()
ZK_PVC_SIZE=""
ZK_PVC_CLASS=""
KAFKA_REPLICAS=0
KAFKA_PVC=()
KAFKA_PVC_SIZE=""
KAFKA_PVC_CLASS=""
COMMAND=""
CLEANUP=true
CONFIRM=true
NAMESPACE=""
CLUSTER_NAME=""
TARGET_FILE=""
SOURCE_FILE=""
FILE_PATH=""
FILE_NAME=""
CUSTOM_CM=""
CUSTOM_SE=""

start_cluster() {
  if [[ -n $NAMESPACE && -n $CLUSTER_NAME ]]; then
    echo "Starting cluster $CLUSTER_NAME"
    CLEANUP=false
    local zoo_ss && zoo_ss="$(kubectl -n "$NAMESPACE" get sts "$CLUSTER_NAME"-zookeeper -o name --ignore-not-found)"
    if [[ -n $zoo_ss ]]; then
      kubectl -n "$NAMESPACE" scale "$zoo_ss" --replicas "$ZK_REPLICAS"
      wait_for "condition=Ready" "pod -l strimzi.io/name=$CLUSTER_NAME-zookeeper"
    fi
    local kafka_ss && kafka_ss="$(kubectl -n "$NAMESPACE" get sts "$CLUSTER_NAME"-kafka -o name --ignore-not-found)"
    if [[ -n $kafka_ss ]]; then
      kubectl -n "$NAMESPACE" scale "$kafka_ss" --replicas "$KAFKA_REPLICAS"
      wait_for "condition=Ready" "pod -l strimzi.io/name=$CLUSTER_NAME-kafka"
    fi
    local eo_deploy && eo_deploy="$(kubectl -n "$NAMESPACE" get deploy "$CLUSTER_NAME"-entity-operator -o name --ignore-not-found)"
    if [[ -n $eo_deploy ]]; then
      kubectl -n "$NAMESPACE" scale "$eo_deploy" --replicas 1
      wait_for "condition=Ready" "pod -l strimzi.io/name=$CLUSTER_NAME-entity-operator"
    fi
    local ke_deploy && ke_deploy="$(kubectl -n "$NAMESPACE" get deploy "$CLUSTER_NAME"-kafka-exporter -o name --ignore-not-found)"
    if [[ -n $ke_deploy ]]; then
      kubectl -n "$NAMESPACE" scale "$ke_deploy" --replicas 1
      wait_for "condition=Ready" "pod -l strimzi.io/name=$CLUSTER_NAME-kafka-exporter"
    fi    
    local co_deploy && co_deploy="$(kubectl -n "$NAMESPACE" get deploy strimzi-cluster-operator -o name --ignore-not-found)"
    if [[ -n $co_deploy ]]; then
      kubectl -n "$NAMESPACE" scale "$co_deploy" --replicas 1
      wait_for "condition=Ready" "pod -l strimzi.io/kind=cluster-operator"
    fi
    wait_for "condition=Ready" "pod -l strimzi.io/name=$CLUSTER_NAME-kafka"
  fi
}

stop_cluster() {
  if [[ -n $NAMESPACE && -n $CLUSTER_NAME ]]; then
    echo "Stopping cluster $CLUSTER_NAME"
    local co_deploy && co_deploy="$(kubectl -n "$NAMESPACE" get deploy strimzi-cluster-operator -o name --ignore-not-found)"
    if [[ -n $co_deploy ]]; then
      kubectl -n "$NAMESPACE" scale "$co_deploy" --replicas 0
      wait_for "delete" "pod -l strimzi.io/kind=cluster-operator"
    else
      echo "No local operator found, make sure no cluster-wide operator is watching this namespace"
      confirm
    fi
    local ke_deploy && ke_deploy="$(kubectl -n "$NAMESPACE" get deploy "$CLUSTER_NAME"-kafka-exporter -o name --ignore-not-found)"
    if [[ -n $ke_deploy ]]; then
      kubectl -n "$NAMESPACE" scale "$ke_deploy" --replicas 0
      wait_for "delete" "pod -l strimzi.io/name=$CLUSTER_NAME-kafka-exporter"
    fi
    local eo_deploy && eo_deploy="$(kubectl -n "$NAMESPACE" get deploy "$CLUSTER_NAME"-entity-operator -o name --ignore-not-found)"
    if [[ -n $eo_deploy ]]; then
      kubectl -n "$NAMESPACE" scale "$eo_deploy" --replicas 0
      wait_for "delete" "pod -l strimzi.io/name=$CLUSTER_NAME-entity-operator"
    fi    
    local kafka_ss && kafka_ss="$(kubectl -n "$NAMESPACE" get sts "$CLUSTER_NAME"-kafka -o name --ignore-not-found)"
    if [[ -n $kafka_ss ]]; then
      kubectl -n "$NAMESPACE" scale "$kafka_ss" --replicas 0
      wait_for "delete" "pod -l strimzi.io/name=$CLUSTER_NAME-kafka"
    fi
    local zoo_ss && zoo_ss="$(kubectl -n "$NAMESPACE" get sts "$CLUSTER_NAME"-zookeeper -o name --ignore-not-found)"
    if [[ -n $zoo_ss ]]; then
      kubectl -n "$NAMESPACE" scale "$zoo_ss" --replicas 0
      wait_for "delete" "pod -l strimzi.io/name=$CLUSTER_NAME-zookeeper"
    fi
    kubectl delete po -l app.kubernetes.io/instance="$CLUSTER_NAME" --ignore-not-found
  fi
}

failure() {
  local ln="$1"
  local cmd="$2"
  if [[ -n $COMMAND && $CLEANUP == true ]]; then
    kubectl -n "$NAMESPACE" delete pod "$RSYNC_POD_NAME" 2>/dev/null ||true
    start_cluster
  fi
  echo "$COMMAND failed at $ln: $cmd"
}

error() {
  echo "$@" 1>&2
  exit 1
}

confirm() {
  read -rp "Please confirm (y/n) " reply
  if [[ ! $reply =~ ^[Yy]$ ]]; then
    CLEANUP=false
    exit 0
  fi
}

check_kube_conn() {
  kubectl version --request-timeout=10s 1>/dev/null
}

wait_for() {
  local condition="$1"
  local resource="$2"
  local namespace="${3-$NAMESPACE}"
  local timeout_sec=300
  if [[ -z $condition || -z $resource || -z $namespace ]]; then
    error "Missing parameters"
  fi
  echo "Waiting for \"$condition\" on \"$resource\" in namespace \"$namespace\""
  local res_cmd="kubectl -n $namespace get $resource -o name --ignore-not-found"
  local con_cmd="kubectl -n $namespace wait --for=$condition $resource --timeout=${timeout_sec}s"
  # waiting for resource
  local i=0; while [[ ! $($res_cmd) && $i -lt $timeout_sec ]]; do
    i=$((i+1)) && sleep 1
  done
  # waiting for condition
  $con_cmd
}

export_env() {
  local dest_path="$1"
  if [[ -z $dest_path ]]; then
    error "Missing parameters"
  fi
  echo "Exporting environment"
  local zk_label="strimzi.io/name=$CLUSTER_NAME-zookeeper"
  ZK_REPLICAS=$(kubectl -n "$NAMESPACE" get kafka "$CLUSTER_NAME" -o yaml | yq eval ".spec.zookeeper.replicas" -)
  mapfile -t ZK_PVC < <(kubectl -n "$NAMESPACE" get pvc -l "$zk_label" -o yaml | yq eval ".items[].metadata.name" -)
  ZK_PVC_SIZE=$(kubectl -n "$NAMESPACE" get pvc -l "$zk_label" -o yaml | yq eval ".items[0].spec.resources.requests.storage" -)
  ZK_PVC_CLASS=$(kubectl -n "$NAMESPACE" get pvc -l "$zk_label" -o yaml | yq eval ".items[0].spec.storageClassName" -)
  local kafka_label="strimzi.io/name=$CLUSTER_NAME-kafka"
  KAFKA_REPLICAS=$(kubectl -n "$NAMESPACE" get kafka "$CLUSTER_NAME" -o yaml | yq eval ".spec.kafka.replicas" -)
  mapfile -t KAFKA_PVC < <(kubectl -n "$NAMESPACE" get pvc -l "$kafka_label" -o yaml | yq eval ".items[].metadata.name" -)
  KAFKA_PVC_SIZE=$(kubectl -n "$NAMESPACE" get pvc -l "$kafka_label" -o yaml | yq eval ".items[0].spec.resources.requests.storage" -)
  KAFKA_PVC_CLASS=$(kubectl -n "$NAMESPACE" get pvc -l "$kafka_label" -o yaml | yq eval ".items[0].spec.storageClassName" -)
  declare -px ZK_REPLICAS ZK_PVC ZK_PVC_SIZE ZK_PVC_CLASS KAFKA_REPLICAS KAFKA_PVC KAFKA_PVC_SIZE KAFKA_PVC_CLASS > "$dest_path"
}

export_res() {
  local res_type="$1"
  local res_id="$2"
  local dest_path="$3"
  if [[ -z $res_type || -z $res_id || -z $dest_path ]]; then
    error "Missing parameters"
  fi
  local resources && resources=$(kubectl -n "$NAMESPACE" get "$res_type" -l "$res_id" -o name --ignore-not-found ||true)
  if [[ -z $resources ]]; then
    resources=$(kubectl -n "$NAMESPACE" get "$res_type" "$res_id" -o name --ignore-not-found 2>/dev/null ||true)
  fi
  local del_metadata="del(.metadata.namespace, .items[].metadata.namespace, \
    .metadata.resourceVersion, .items[].metadata.resourceVersion, \
    .metadata.selfLink, .items[].metadata.selfLink, \
    .metadata.uid, .items[].metadata.uid, \
    .status, .items[].status)"
  for res in $resources; do
    echo "Exporting $res"
    local file_name && file_name=$(echo "$res" | sed 's/\//-/g;s/ //g')
    kubectl -n "$NAMESPACE" get "$res" -o yaml | yq eval "$del_metadata" - > "$dest_path/$file_name.yaml"
  done
}

rsync() {
  local source="$1"
  local target="$2"
  if [[ -z $source || -z $target ]]; then
    error "Missing parameters"
  fi
  echo "Rsync from $source to $target"
  local flags="--no-check-device --no-acls --no-xattrs --no-same-owner --warning=no-file-changed"
  if [[ $source != "$FILE_PATH/"* ]]; then
    # download from pod to local (backup)
    flags="$flags --exclude=data/version-2/{currentEpoch,acceptedEpoch}"
    local patch && patch=$(sed "s@\$name@$source@g" "$BASE/templates/patch.json")
    kubectl -n "$NAMESPACE" run "$RSYNC_POD_NAME" --image "dummy" --restart "Never" --overrides "$patch"
    wait_for "condition=Ready" "pod -l run=$RSYNC_POD_NAME"
    # double quotes breaks tar commands
    # shellcheck disable=SC2086
    kubectl -n "$NAMESPACE" exec -i "$RSYNC_POD_NAME" -- tar $flags -C /data -c . \
      | tar $flags -C $target -xv -f - && if [[ $? == 1 ]]; then exit 0; fi
    kubectl -n "$NAMESPACE" delete pod "$RSYNC_POD_NAME"
  else
    # upload from local to pod (restore)
    local patch && patch=$(sed "s@\$name@$target@g" "$BASE/templates/patch.json")
    kubectl -n "$NAMESPACE" run "$RSYNC_POD_NAME" --image "dummy" --restart "Never" --overrides "$patch"
    wait_for "condition=ready" "pod -l run=$RSYNC_POD_NAME"
    # double quotes breaks tar commands
    # shellcheck disable=SC2086
    tar $flags -C $source -c . \
      | kubectl -n "$NAMESPACE" exec -i "$RSYNC_POD_NAME" -- tar $flags -C /data -xv -f -
    kubectl -n "$NAMESPACE" delete pod "$RSYNC_POD_NAME"
  fi
}

create_pvc() {
  local name="$1"
  local size="$2"
  local class="${3-}"
  if [[ -z $name || -z $size ]]; then
    error "Missing parameters"
  fi
  local exp="s/\$name/$name/g; s/\$size/$size/g; /storageClassName/d"
  if [[ $class != "null" ]]; then
    exp="s/\$name/$name/g; s/\$size/$size/g; s/\$class/$class/g"
  fi
  echo "Creating pvc $name of size $size"
  sed "$exp" "$BASE"/templates/pvc.yaml | kubectl -n "$NAMESPACE" create -f -
}

compress() {
  local source_dir="$1"
  local target_file="$2"
  if [[ -z $source_dir || -z $target_file ]]; then
    error "Missing parameters"
  fi
  echo "Compressing $source_dir to $target_file"
  tar -czf "$target_file" -C "$source_dir" .
}

uncompress() {
  local source_file="$1"
  local target_dir="$2"
  if [[ -z $source_file || -z $target_dir ]]; then
    error "Missing parameters"
  fi
  echo "Uncompressing $source_file to $target_dir"
  mkdir -p "$target_dir"
  tar -xzf "$source_file" -C "$target_dir"
  chmod -R ugo+rwx "$target_dir"
}

backup() {
  if [[ -z $NAMESPACE || -z $CLUSTER_NAME || -z $TARGET_FILE ]]; then
    CLEANUP=false
    error "Specify namespace, cluster name and target file to backup"
  fi
  if [[ ! -d "$(dirname "$TARGET_FILE")" ]]; then
    CLEANUP=false
    error "$(dirname "$TARGET_FILE") not found"
  fi

  local tmp="$FILE_PATH/$NAMESPACE/$CLUSTER_NAME"
  if [[ -n "$(ls -A $tmp 2>/dev/null ||true)" ]]; then
    CLEANUP=false
    error "Non empty directory: $tmp"
  fi
  
  if [[ $CONFIRM == true ]]; then
    echo "Backup of cluster $NAMESPACE/$CLUSTER_NAME"
    echo "The cluster won't be available for the entire duration of the process"
    confirm
  fi
  
  mkdir -p "$tmp/resources" "$tmp/data"
  export_env "$tmp/log-dump-env"
  check_kube_conn
  stop_cluster

  # export resources
  export_res kafka "$CLUSTER_NAME" "$tmp"/resources
  export_res kafkatopics "strimzi.io/cluster=$CLUSTER_NAME" "$tmp/resources"
  export_res kafkausers "strimzi.io/cluster=$CLUSTER_NAME" "$tmp/resources"
  export_res kafkarebalances "strimzi.io/cluster=$CLUSTER_NAME" "$tmp/resources"
  # cluster configmap and secrets
  export_res configmaps "app.kubernetes.io/instance=$CLUSTER_NAME" "$tmp/resources"
  export_res secrets "app.kubernetes.io/instance=$CLUSTER_NAME" "$tmp/resources"
  # custom configmap and secrets
  if [[ -n $CUSTOM_CM ]]; then
    for name in ${CUSTOM_CM//,/ }; do
      export_res configmap "$name" "$tmp"/resources
    done
  fi
  if [[ -n $CUSTOM_SE ]]; then
    for name in ${CUSTOM_SE//,/ }; do
      export_res secret "$name" "$tmp"/resources
    done
  fi

  # for each PVC, rsync data from PV to backup
  for name in "${ZK_PVC[@]}"; do
    local path="$tmp/data/$name"
    mkdir -p "$path"
    rsync "$name" "$path"
  done
  for name in "${KAFKA_PVC[@]}"; do
    local path="$tmp/data/$name"
    mkdir -p "$path"
    rsync "$name" "$path"
  done

  # create the archive
  compress "$tmp" "$TARGET_FILE"
  start_cluster
  echo "$COMMAND completed successfully"
}

restore() {
  if  [[ -z $NAMESPACE || -z $CLUSTER_NAME || -z $SOURCE_FILE ]]; then
    CLEANUP=false
    error "Specify namespace, cluster name and source file to restore"
  fi
  if [[ ! -f $SOURCE_FILE ]]; then
    CLEANUP=false
    error "$SOURCE_FILE file not found"
  fi
  
  check_kube_conn
  if [[ -z $(kubectl get ns "$NAMESPACE" -o name --ignore-not-found) ]]; then
    CLEANUP=false
    error "Namespace not found"
  fi

  if [[ $CONFIRM == true ]]; then
    echo "Restore of cluster $NAMESPACE/$CLUSTER_NAME"
    confirm
  fi
  
  local tmp="$FILE_PATH/$NAMESPACE/$CLUSTER_NAME"
  uncompress "$SOURCE_FILE" "$tmp"
  # do not access external source file
  # shellcheck source=/dev/null 
  source "$tmp/log-dump-env"
  stop_cluster

  # for each PVC, create it and rsync data from backup to PV
  for name in "${ZK_PVC[@]}"; do
    create_pvc "$name" "$ZK_PVC_SIZE" "$ZK_PVC_CLASS"
    rsync "$tmp/data/$name/." "$name"
  done
  for name in "${KAFKA_PVC[@]}"; do
    create_pvc "$name" "$KAFKA_PVC_SIZE" "$KAFKA_PVC_CLASS"
    rsync "$tmp/data/$name/." "$name"
  done

  # import resources
  # KafkaTopic resources must be created *before*
  # deploying the Topic Operator or it will delete them
  kubectl -n "$NAMESPACE" apply -f "$tmp"/resources
  start_cluster
  echo "$COMMAND completed successfully"
}

readonly USAGE="
Usage: $0 [commands] [options]

Commands:
  backup   Cluster backup
  restore  Cluster restore

Options:
  -y  Skip confirmation step
  -n  Cluster namespace
  -c  Cluster name
  -t  Target file path (tgz)
  -s  Source file path (tgz)
  -m  Custom config maps (-m cm0,cm1,cm2)
  -x  Custom secrets (-x se0,se1,se2)
"
readonly PARAMS=("$@")
i=0; for param in "${PARAMS[@]}"; do
  i=$((i+1))
  case $param in
    -y)
      CONFIRM=false && readonly CONFIRM && export CONFIRM
      ;;
    -n)
      NAMESPACE=${PARAMS[i]} && readonly NAMESPACE && export NAMESPACE
      ;;
    -c)
      CLUSTER_NAME=${PARAMS[i]} && readonly CLUSTER_NAME && export CLUSTER_NAME
      ;;
    -t)
      TARGET_FILE=${PARAMS[i]} && readonly TARGET_FILE && export TARGET_FILE
      FILE_PATH=$(dirname "$TARGET_FILE") && readonly FILE_PATH && export FILE_PATH
      FILE_NAME=$(basename "$TARGET_FILE") && readonly FILE_NAME && export FILE_NAME
      ;;
    -s)
      SOURCE_FILE=${PARAMS[i]} && readonly SOURCE_FILE && export SOURCE_FILE
      FILE_PATH=$(dirname "$SOURCE_FILE") && readonly FILE_PATH && export FILE_PATH
      FILE_NAME=$(basename "$SOURCE_FILE") && readonly FILE_NAME && export FILE_NAME
      ;;
    -m)
      CUSTOM_CM=${PARAMS[i]} && readonly CUSTOM_CM && export CUSTOM_CM
      ;;
    -x)
      CUSTOM_SE=${PARAMS[i]} && readonly CUSTOM_SE && export CUSTOM_SE
      ;;
  esac
done
readonly COMMAND="${1-}"
if [[ -z "$COMMAND" ]]; then
  error "$USAGE"
else
  if (declare -F "$COMMAND" >/dev/null); then
    "$COMMAND"
  else
    error "Invalid command"
  fi
fi
