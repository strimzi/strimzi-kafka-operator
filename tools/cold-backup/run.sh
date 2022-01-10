#!/usr/bin/env bash
set -Eeuo pipefail
if [[ $(uname -s) == "Darwin" ]]; then
  shopt -s expand_aliases
  alias echo="gecho"; alias dirname="gdirname"; alias grep="ggrep"; alias readlink="greadlink"
  alias tar="gtar"; alias sed="gsed"; alias date="gdate"; alias ls="gls"
fi
BASE="" && pushd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" >/dev/null \
  && { BASE=$PWD; popd >/dev/null; } && readonly BASE
trap cleanup EXIT

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
BACKUP_PATH=""
BACKUP_NAME=""
CUSTOM_CM=""
CUSTOM_SE=""

start_cluster() {
  if [[ -n $NAMESPACE && -n $CLUSTER_NAME ]]; then
    echo "Starting cluster $CLUSTER_NAME"
    if [[ $COMMAND == "backup" && -n $ZK_REPLICAS && -n $KAFKA_REPLICAS ]]; then
      local zoo_ss="$(kubectl -n $NAMESPACE get statefulset $CLUSTER_NAME-zookeeper -o name --ignore-not-found)"
      if [[ -n $zoo_ss ]]; then
        kubectl -n $NAMESPACE scale $zoo_ss --replicas $ZK_REPLICAS
        wait_for condition=ready pod "strimzi.io/name=$CLUSTER_NAME-zookeeper"
      fi
      local kafka_ss="$(kubectl -n $NAMESPACE get statefulset $CLUSTER_NAME-kafka -o name --ignore-not-found)"
      if [[ -n $kafka_ss ]]; then
        kubectl -n $NAMESPACE scale $kafka_ss --replicas $KAFKA_REPLICAS
        wait_for condition=ready pod "strimzi.io/name=$CLUSTER_NAME-kafka"
      fi
      local eo_deploy="$(kubectl -n $NAMESPACE get deploy $CLUSTER_NAME-entity-operator -o name --ignore-not-found)"
      if [[ -n $eo_deploy ]]; then
        kubectl -n $NAMESPACE scale $eo_deploy --replicas 1
        wait_for condition=ready pod "strimzi.io/name=$CLUSTER_NAME-entity-operator"
      fi
      local ke_deploy="$(kubectl -n $NAMESPACE get deploy $CLUSTER_NAME-kafka-exporter -o name --ignore-not-found)"
      if [[ -n $ke_deploy ]]; then
        kubectl -n $NAMESPACE scale $ke_deploy --replicas 1
        wait_for condition=ready pod "strimzi.io/name=$CLUSTER_NAME-kafka-exporter"
      fi
    fi
    if [[ $COMMAND == "backup" || $COMMAND == "restore" ]]; then
      local co_deploy="$(kubectl -n $NAMESPACE get deploy strimzi-cluster-operator -o name --ignore-not-found)"
      if [[ -n $co_deploy ]]; then
        kubectl -n $NAMESPACE scale $co_deploy --replicas 1
        wait_for condition=ready pod "strimzi.io/kind=cluster-operator"
      fi
      if [[ $COMMAND == "restore" ]]; then
        wait_for condition=ready pod "strimzi.io/name=$CLUSTER_NAME-kafka"
      fi
    fi
  fi
}

stop_cluster() {
  if [[ -n $NAMESPACE && -n $CLUSTER_NAME ]]; then
    echo "Stopping cluster $CLUSTER_NAME"
    local co_deploy="$(kubectl -n $NAMESPACE get deploy strimzi-cluster-operator -o name --ignore-not-found)"
    if [[ -n $co_deploy ]]; then
      kubectl -n $NAMESPACE scale $co_deploy --replicas 0
      wait_for delete pod "strimzi.io/kind=cluster-operator"
    else
      echo "No local operator found, make sure no cluster-wide operator is watching this namespace"
      confirm
    fi
    local eo_deploy="$(kubectl -n $NAMESPACE get deploy $CLUSTER_NAME-entity-operator -o name --ignore-not-found)"
    if [[ -n $eo_deploy ]]; then
      kubectl -n $NAMESPACE scale $eo_deploy --replicas 0
      wait_for delete pod "strimzi.io/name=$CLUSTER_NAME-entity-operator"
    fi
    local ke_deploy="$(kubectl -n $NAMESPACE get deploy $CLUSTER_NAME-kafka-exporter -o name --ignore-not-found)"
    if [[ -n $ke_deploy ]]; then
      kubectl -n $NAMESPACE scale $ke_deploy --replicas 0
      wait_for delete pod "strimzi.io/name=$CLUSTER_NAME-kafka-exporter"
    fi
    local kafka_ss="$(kubectl -n $NAMESPACE get statefulset $CLUSTER_NAME-kafka -o name --ignore-not-found)"
    if [[ -n $kafka_ss ]]; then
      kubectl -n $NAMESPACE scale $kafka_ss --replicas 0
      wait_for delete pod "strimzi.io/name=$CLUSTER_NAME-kafka"
    fi
    local zoo_ss="$(kubectl -n $NAMESPACE get statefulset $CLUSTER_NAME-zookeeper -o name --ignore-not-found)"
    if [[ -n $zoo_ss ]]; then
      kubectl -n $NAMESPACE scale $zoo_ss --replicas 0
      wait_for delete pod "strimzi.io/name=$CLUSTER_NAME-zookeeper"
    fi
  fi
}

cleanup() {
  if [[ -n $COMMAND && $CLEANUP == true ]]; then
    kubectl -n $NAMESPACE delete pod $RSYNC_POD_NAME 2>/dev/null ||true
    start_cluster
  fi
}

error() {
  echo "$@" 1>&2
  exit 1
}

confirm() {
  read -p "Please confirm (y/n) " reply
  if [[ ! $reply =~ ^[Yy]$ ]]; then
    CLEANUP=false
    exit 0
  fi
}

check_kube_conn() {
  kubectl version --request-timeout=10s 1>/dev/null
}

wait_for() {
  local cond="$1"
  local resource="$2"
  local label="${3-}"
  local timeout_sec=300
  if [[ -z $cond || -z $resource ]]; then
    error "Missing parameters"
  fi
  if [[ -n $label ]]; then
    resource="$resource -l $label"
  fi
  echo "Waiting for $cond on $resource"
  local res_cmd="kubectl -n $NAMESPACE get $resource -o name --ignore-not-found"
  local wait_cmd="kubectl -n $NAMESPACE wait --for=$cond $resource --timeout=${timeout_sec}s"
  # wait for resource
  local i=0; while [[ ! $($res_cmd) && $i -lt $timeout_sec ]]; do
    i=$((i+1)) && sleep 1
  done
  # wait for condition
  $wait_cmd
}

export_env() {
  local dest_path="$1"
  if [[ -z $dest_path ]]; then
    error "Missing parameters"
  fi
  echo "Exporting environment"
  local zk_label="strimzi.io/name=$CLUSTER_NAME-zookeeper"
  ZK_REPLICAS=$(kubectl -n $NAMESPACE get kafka $CLUSTER_NAME -o yaml | yq eval ".spec.zookeeper.replicas" -)
  mapfile -t ZK_PVC < <(kubectl -n $NAMESPACE get pvc -l $zk_label -o yaml | yq eval ".items[].metadata.name" -)
  ZK_PVC_SIZE=$(kubectl -n $NAMESPACE get pvc -l $zk_label -o yaml | yq eval ".items[0].spec.resources.requests.storage" -)
  ZK_PVC_CLASS=$(kubectl -n $NAMESPACE get pvc -l $zk_label -o yaml | yq eval ".items[0].spec.storageClassName" -)
  local kafka_label="strimzi.io/name=$CLUSTER_NAME-kafka"
  KAFKA_REPLICAS=$(kubectl -n $NAMESPACE get kafka $CLUSTER_NAME -o yaml | yq eval ".spec.kafka.replicas" -)
  mapfile -t KAFKA_PVC < <(kubectl -n $NAMESPACE get pvc -l $kafka_label -o yaml | yq eval ".items[].metadata.name" -)
  KAFKA_PVC_SIZE=$(kubectl -n $NAMESPACE get pvc -l $kafka_label -o yaml | yq eval ".items[0].spec.resources.requests.storage" -)
  KAFKA_PVC_CLASS=$(kubectl -n $NAMESPACE get pvc -l $kafka_label -o yaml | yq eval ".items[0].spec.storageClassName" -)
  declare -px ZK_REPLICAS ZK_PVC ZK_PVC_SIZE ZK_PVC_CLASS KAFKA_REPLICAS KAFKA_PVC KAFKA_PVC_SIZE KAFKA_PVC_CLASS > "$dest_path"
}

export_res() {
  local res_type="$1"
  local res_id="$2"
  local dest_path="$3"
  if [[ -z $res_type || -z $res_id || -z $dest_path ]]; then
    error "Missing parameters"
  fi
  local resources=$(kubectl -n $NAMESPACE get $res_type -l $res_id -o name --ignore-not-found ||true)
  if [[ -z $resources ]]; then
    resources=$(kubectl -n $NAMESPACE get $res_type $res_id -o name --ignore-not-found 2>/dev/null ||true)
  fi
  if [[ -n $resources ]]; then
    echo "Exporting $res_type $res_id"
    # delete runtime metadata expression
    local exp="del(.metadata.namespace, .items[].metadata.namespace, \
      .metadata.resourceVersion, .items[].metadata.resourceVersion, \
      .metadata.selfLink, .items[].metadata.selfLink, \
      .metadata.uid, .items[].metadata.uid, \
      .status, .items[].status)"
    local file_name=$(printf $res_type-$res_id | sed 's/\//-/g;s/ //g')
    kubectl -n $NAMESPACE get $resources -o yaml | yq eval "$exp" - > $dest_path/$file_name.yaml
  fi
}

rsync() {
  local source="$1"
  local target="$2"
  if [[ -z $source || -z $target ]]; then
    error "Missing parameters"
  fi
  echo "Rsync from $source to $target"
  local flags="--no-check-device --no-acls --no-xattrs --no-same-owner --warning=no-file-changed"
  if [[ $source != "$BACKUP_PATH/"* ]]; then
    # download from pod to local (backup)
    flags="$flags --exclude=data/version-2/{currentEpoch,acceptedEpoch}"
    local patch=$(sed "s@\$name@$source@g" $BASE/templates/patch.json)
    kubectl -n $NAMESPACE run $RSYNC_POD_NAME --image "dummy" --restart "Never" --overrides "$patch"
    wait_for condition=ready pod "run=$RSYNC_POD_NAME"
    # ignore the sporadic file changed error
    kubectl -n $NAMESPACE exec -i $RSYNC_POD_NAME -- sh -c "tar $flags -C /data -c ." \
      | tar $flags -C $target -xv -f - && if [[ $? == 1 ]]; then exit 0; fi
    kubectl -n $NAMESPACE delete pod $RSYNC_POD_NAME
  else
    # upload from local to pod (restore)
    local patch=$(sed "s@\$name@$target@g" $BASE/templates/patch.json)
    kubectl -n $NAMESPACE run $RSYNC_POD_NAME --image "dummy" --restart "Never" --overrides "$patch"
    wait_for condition=ready pod "run=$RSYNC_POD_NAME"
    tar $flags -C $source -c . \
      | kubectl -n $NAMESPACE exec -i $RSYNC_POD_NAME -- sh -c "tar $flags -C /data -xv -f -"
    kubectl -n $NAMESPACE delete pod $RSYNC_POD_NAME
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
  sed "$exp" $BASE/templates/pvc.yaml | kubectl -n $NAMESPACE create -f -
}

compress() {
  local source_dir="$1"
  local target_file="$2"
  if [[ -z $source_dir || -z $target_file ]]; then
    error "Missing parameters"
  fi
  local current_dir=$(pwd)
  cd $source_dir
  echo "Compressing $source_dir to $target_file"
  zip -FSqr $BACKUP_NAME *
  mv $BACKUP_NAME $BACKUP_PATH
  cd $current_dir
}

uncompress() {
  local source_file="$1"
  local target_dir="$2"
  if [[ -z $source_file || -z $target_dir ]]; then
    error "Missing parameters" 
  fi
  echo "Uncompressing $source_file to $target_dir"
  mkdir -p $target_dir
  unzip -qo $source_file -d $target_dir
  chmod -R ugo+rwx $target_dir
}

backup() {
  if [[ -z $NAMESPACE || -z $CLUSTER_NAME || -z $TARGET_FILE ]]; then
    CLEANUP=false
    error "Specify namespace, cluster name and target file to backup"
  fi
  if [[ ! -d "$(dirname $TARGET_FILE)" ]]; then
    error "$(dirname $TARGET_FILE) not found"
  fi
  
  # init context
  local tmp="$BACKUP_PATH/$NAMESPACE/$CLUSTER_NAME"
  if [[ ! -z "$(ls -A $tmp 2>/dev/null ||true)" ]]; then
    CLEANUP=false
    error "Non empty directory: $tmp"
  fi
  check_kube_conn
  if [[ $CONFIRM == true ]]; then
    echo "Backup of cluster $NAMESPACE/$CLUSTER_NAME"
    echo "The cluster won't be available for the entire duration of the process"
    confirm
  fi
  echo "Starting backup"
  mkdir -p $tmp/resources $tmp/data
  export_env $tmp/env
  stop_cluster

  # export resources
  export_res kafka $CLUSTER_NAME $tmp/resources
  export_res kafkatopics strimzi.io/cluster=$CLUSTER_NAME $tmp/resources
  export_res kafkausers strimzi.io/cluster=$CLUSTER_NAME $tmp/resources
  export_res kafkarebalances strimzi.io/cluster=$CLUSTER_NAME $tmp/resources
  # cluster configmap and secrets
  export_res configmaps app.kubernetes.io/instance=$CLUSTER_NAME $tmp/resources
  export_res secrets app.kubernetes.io/instance=$CLUSTER_NAME $tmp/resources
  # custom configmap and secrets
  if [[ -n $CUSTOM_CM ]]; then
    for name in $(printf $CUSTOM_CM | sed "s/,/ /g"); do
      export_res configmap $name $tmp/resources
    done
  fi
  if [[ -n $CUSTOM_SE ]]; then
    for name in $(printf $CUSTOM_SE | sed "s/,/ /g"); do
      export_res secret $name $tmp/resources
    done
  fi

  # for each PVC, rsync data from PV to backup
  for name in "${ZK_PVC[@]}"; do
    local path="$tmp/data/$name"
    mkdir -p $path
    rsync $name $path
  done
  for name in "${KAFKA_PVC[@]}"; do
    local path="$tmp/data/$name"
    mkdir -p $path
    rsync $name $path
  done

  # create the archive
  compress $tmp $TARGET_FILE
}

restore() {
  if  [[ -z $NAMESPACE || -z $CLUSTER_NAME || -z $SOURCE_FILE ]]; then
    CLEANUP=false
    error "Specify namespace, cluster name and source file to restore"
  fi
  if [[ ! -f $SOURCE_FILE ]]; then
    error "$SOURCE_FILE file not found"
  fi
  
  # init context
  local tmp="$BACKUP_PATH/$NAMESPACE/$CLUSTER_NAME"
  check_kube_conn
  if [[ $CONFIRM == true ]]; then
    echo "Restore of cluster $NAMESPACE/$CLUSTER_NAME"
    confirm
  fi
  uncompress $SOURCE_FILE $tmp
  source $tmp/env
  stop_cluster

  # for each PVC, create it and rsync data from backup to PV
  for name in "${ZK_PVC[@]}"; do
    create_pvc $name $ZK_PVC_SIZE $ZK_PVC_CLASS
    rsync $tmp/data/$name/. $name
  done
  for name in "${KAFKA_PVC[@]}"; do
    create_pvc $name $KAFKA_PVC_SIZE $KAFKA_PVC_CLASS
    rsync $tmp/data/$name/. $name
  done

  # import resources
  # KafkaTopic resources must be created *before*
  # deploying the Topic Operator or it will delete them
  kubectl -n $NAMESPACE apply -f $tmp/resources
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
  -t  Target file path
  -s  Source file path
  -m  Custom config maps (-m cm0,cm1,cm2)
  -x  Custom secrets (-x se0,se1,se2)

Examples:
  # Full backup with custom cm and secrets
  $0 backup -n test -c my-cluster \\
    -t /tmp/my-cluster.zip \\
    -m kafka-metrics,log4j-properties \\
    -x ext-listener-crt

  # Restore to a new namespace
  $0 restore -n test-new -c my-cluster \\
    -s /tmp/my-cluster.zip
"
readonly PARAMS="${@}"
readonly PARRAY=($PARAMS)
i=0; for param in $PARAMS; do
  i=$(($i+1))
  case $param in
    -y)
      readonly CONFIRM=false && export CONFIRM
      ;;
    -n)
      readonly NAMESPACE=${PARRAY[i]} && export NAMESPACE
      ;;
    -c)
      readonly CLUSTER_NAME=${PARRAY[i]} && export CLUSTER_NAME
      ;;
    -t)
      readonly TARGET_FILE=${PARRAY[i]} && export TARGET_FILE
      readonly BACKUP_PATH=$(dirname $TARGET_FILE) && export BACKUP_PATH
      readonly BACKUP_NAME=$(basename $TARGET_FILE) && export BACKUP_NAME
      ;;
    -s)
      readonly SOURCE_FILE=${PARRAY[i]} && export SOURCE_FILE
      readonly BACKUP_PATH=$(dirname $SOURCE_FILE) && export BACKUP_PATH
      readonly BACKUP_NAME=$(basename $SOURCE_FILE) && export BACKUP_NAME
      ;;
    -m)
      readonly CUSTOM_CM=${PARRAY[i]} && export CUSTOM_CM
      ;;
    -x)
      readonly CUSTOM_SE=${PARRAY[i]} && export CUSTOM_SE
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
