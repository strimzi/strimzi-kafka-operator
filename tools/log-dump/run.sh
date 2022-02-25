#!/usr/bin/env bash
set -Eeuo pipefail
if [[ $(uname -s) == "Darwin" ]]; then
  shopt -s expand_aliases
  alias echo="gecho"; alias dirname="gdirname"; alias grep="ggrep"; alias readlink="greadlink"
  alias tar="gtar"; alias sed="gsed"; alias date="gdate"; alias wc="gwc"
fi

readonly CO_TOPIC="__consumer_offsets"
readonly TS_TOPIC="__transaction_state"
readonly CM_TOPIC="__cluster_metadata"
KAFKA_BROKERS=0
STORAGE_TYPE=""
JBOD_DISKS=0
COMMAND=""
OUT_PATH="/tmp/log-dump"
NAMESPACE=""
CLUSTER=""
TOPIC=""
PARTITION=0
SEGMENT=""
GROUP_ID=""
TXN_ID=""
TOT_PART=50
DRY_RUN=false
DATA=false

error() {
  echo "$@" 1>&2
  exit 1
}

check_number() {
  local value="$1"
  local regex='^[0-9]+$'
  if ! [[ $value =~ $regex ]]; then
    error "Not a number"
  fi
}

check_kube_conn() {
  kubectl version --request-timeout=10s 1>/dev/null
}

get_kafka_setup() {
  local kafka_yaml && kafka_yaml=$(kubectl -n "$NAMESPACE" get kafka "$CLUSTER" -o yaml ||true)
  KAFKA_BROKERS=$(echo "$kafka_yaml" | yq eval ".spec.kafka.replicas" -)
  STORAGE_TYPE=$(echo "$kafka_yaml" | yq eval ".spec.kafka.storage.type" -)
  JBOD_DISKS=$(echo "$kafka_yaml" | yq eval ".spec.kafka.storage.volumes | length" -)
  if [[ -n $KAFKA_BROKERS && $KAFKA_BROKERS != "null" ]]; then
    echo "brokers: $KAFKA_BROKERS, storage: $STORAGE_TYPE, disks: $JBOD_DISKS"
  else
    error "Kafka cluster $CLUSTER not found in namespace $NAMESPACE"
  fi
}

coord_partition() {
  local id="$1"
  local part="${2-$TOT_PART}"
  if [[ -n $id && -n $part ]]; then
    echo 'public void run(String id, int part) { System.out.println(abs(id.hashCode()) % part); }    
      private int abs(int n) { return (n == Integer.MIN_VALUE) ? 0 : Math.abs(n); } 
      run("'"$id"'", '"$part"');' \
      | jshell -
  fi
}

find_segments() {
  local broker="$1"
  local log_dir="$2"
  if [[ -z $broker || -z $log_dir ]]; then
    error "Missing parameters"
  fi
  local seg_files && seg_files=$(kubectl -n "$NAMESPACE" exec "$CLUSTER"-kafka-"$broker" -- \
    find "$log_dir" -printf "%f\n" 2>/dev/null | grep ".log")
  echo "$seg_files"
}

dump_segments() {
  local seg_files="$1"
  if [[ -z $seg_files || $(echo "$seg_files" | sed '/^\s*$/d' | wc -l) -eq 0 ]]; then
    echo "No segment found"
  fi
  local flags="--deep-iteration"
  case "$topic" in
    "$CO_TOPIC")
      flags="$flags --offsets-decoder"
      ;;
    "$TS_TOPIC")
      flags="$flags --transaction-log-decoder"
      ;;
    "$CM_TOPIC")
      flags="$flags --cluster-metadata-decoder"
      ;;
  esac
  flags="$flags --files"
  for seg_file in $seg_files; do
    if [[ -n $SEGMENT && $SEGMENT != "${seg_file%.log}" ]]; then
      continue
    fi
    echo "$seg_file"
    if [[ $DRY_RUN == false ]]; then
      mkdir -p "$out_dir"
      kubectl -n "$NAMESPACE" exec "$CLUSTER"-kafka-"$i" -- \
        bash -c "./bin/kafka-dump-log.sh $flags $log_dir/$seg_file" > "$out_dir/$seg_file"
    fi
  done
}

dump_partition() {
  local topic="$1"
  local partition="$2"
  local broker="$3"
  local disk="${4-}"
  if [[ -z $topic || -z $partition || -z $broker ]]; then
    error "Missing parameters"
  fi
  # context setup
  local disk_label="$topic-$partition segments in kafka-$broker"
  local log_dir="/var/lib/kafka/data/kafka-log$broker/$topic-$partition"
  local out_dir="$OUT_PATH/$topic/kafka-$broker-$topic-$partition"
  if [[ -n $disk && $disk -ge 0 ]]; then
    disk_label="$topic-$partition segments in kafka-$broker-disk-$disk"
    log_dir="/var/lib/kafka/data-$disk/kafka-log$broker/$topic-$partition";
    out_dir="$OUT_PATH/$topic/kafka-$broker-disk-$disk-$topic-$partition"
  fi
  # segment dump
  local seg_files && seg_files=$(find_segments "$broker" "$log_dir")
  echo "$disk_label"
  dump_segments "$seg_files"
}

partition() {
  if [[ -z $NAMESPACE || -z $CLUSTER || -z $TOPIC || -z $PARTITION ]]; then
    error "Missing parameters"
  fi  
  check_kube_conn
  get_kafka_setup  
  # dump topic partition across the cluster (including replicas)
  for i in $(seq 0 $((KAFKA_BROKERS-1))); do
    if [[ $STORAGE_TYPE == "jbod" ]]; then
      for j in $(seq 0 $((JBOD_DISKS-1))); do
        dump_partition "$TOPIC" "$PARTITION" "$i" "$j"
      done
    else 
      dump_partition "$TOPIC" "$PARTITION" "$i"
    fi
  done
}

cg_offsets() {
  if [[ -z $NAMESPACE || -z $CLUSTER || -z $GROUP_ID ]]; then
    error "Missing parameters"
  fi
  check_kube_conn
  get_kafka_setup
  # dump CG offsets coordinating partition across the cluster (including replicas)
  local partition && partition=$(coord_partition "$GROUP_ID" "$TOT_PART")
  echo "$GROUP_ID coordinating partition: $partition"
  for i in $(seq 0 $((KAFKA_BROKERS-1))); do
    if [[ $STORAGE_TYPE == "jbod" ]]; then
      for j in $(seq 0 $((JBOD_DISKS-1))); do
        dump_partition "$CO_TOPIC" "$partition" "$i" "$j"
      done
    else
      dump_partition "$CO_TOPIC" "$partition" "$i"
    fi
  done
}

txn_state() {
  if [[ -z $NAMESPACE || -z $CLUSTER || -z $TXN_ID ]]; then
    error "Missing parameters"
  fi
  check_kube_conn
  get_kafka_setup    
  # dump TX state coordinating partition across the cluster (including replicas)
  local partition && partition=$(coord_partition "$TXN_ID" "$TOT_PART")
  echo "$TXN_ID coordinating partition: $partition"
  for i in $(seq 0 $((KAFKA_BROKERS-1))); do
    if [[ $STORAGE_TYPE == "jbod" ]]; then
      for j in $(seq 0 $((JBOD_DISKS-1))); do
        dump_partition "$TS_TOPIC" "$partition" "$i" "$j"
      done
    else
      dump_partition "$TS_TOPIC" "$partition" "$i"
    fi
  done
}

cluster_meta() {
  if [[ -z $NAMESPACE || -z $CLUSTER ]]; then
    error "Missing parameters"
  fi
  check_kube_conn
  get_kafka_setup
  # dump cluster metadata partition across the cluster (including replicas)
  local partition="0"
  for i in $(seq 0 $((KAFKA_BROKERS-1))); do
    if [[ $STORAGE_TYPE == "jbod" ]]; then
      for j in $(seq 0 $((JBOD_DISKS-1))); do
        dump_partition "$CM_TOPIC" "$partition" "$i" "$j"
      done
    else
      dump_partition "$CM_TOPIC" "$partition" "$i"
    fi
  done
}

readonly USAGE="
Usage: $0 [command] [params]

  partition       Dump topic partition
    --namespace     Kubernetes namespace
    --cluster       Kafka cluster name
    --topic         Data topic name
    --partition     Partition number (zero-based)
    --segment       Filter by segment name
    --out-path      Output path (default: $OUT_PATH)
    --dry-run       Run without dumping (default: $DRY_RUN)
    --data          Include record payload (default: $DATA)

  cg_offsets      Dump consumer group offsets partition by group.id
    --namespace     Kubernetes namespace
    --cluster       Kafka cluster name
    --group-id      Consumer group id
    --tot-part      Number of __consumer_offsets partitions (default: $TOT_PART)
    --out-path      Output path (default: $OUT_PATH)
    --dry-run       Run without dumping (default: $DRY_RUN)
  
  txn_state       Dump transactions state partition by transactional.id
    --namespace     Kubernetes namespace
    --cluster       Kafka cluster name
    --txn-id        Transactional id
    --tot-part      Number of __transaction_state partitions (default: $TOT_PART)
    --out-path      Output path (default: $OUT_PATH)
    --dry-run       Run without dumping (default: $DRY_RUN)
    
  cluster_meta    Dump cluster metadata (KRaft)
    --namespace     Kubernetes namespace
    --cluster       Kafka cluster name
    --out-path      Output path (default: $OUT_PATH)
    --dry-run       Run without dumping (default: $DRY_RUN)
"
readonly PARAMS=("$@")
i=0; for param in "${PARAMS[@]}"; do
  i=$((i+1))
  case $param in
    --namespace)
      readonly NAMESPACE=${PARAMS[i]} && export NAMESPACE
      ;;
    --cluster)
      readonly CLUSTER=${PARAMS[i]} && export CLUSTER
      ;;
    --topic)
      readonly TOPIC=${PARAMS[i]} && export TOPIC
      ;;
    --partition)
      readonly PARTITION=${PARAMS[i]} && export PARTITION
      check_number "$PARTITION"
      ;;
    --segment)
      readonly SEGMENT=${PARAMS[i]} && export SEGMENT
      ;;
    --group-id)
      readonly GROUP_ID=${PARAMS[i]} && export GROUP_ID
      ;;
    --txn-id)
      readonly TXN_ID=${PARAMS[i]} && export TXN_ID
      ;;
    --tot-part)
      readonly TOT_PART=${PARAMS[i]} && export TOT_PART
      check_number "$TOT_PART"
      ;;
    --out-path)
      readonly OUT_PATH=${PARAMS[i]} && export OUT_PATH
      ;;
    --dry-run)
      readonly DRY_RUN=true && export DRY_RUN
      ;;
    --data)
      readonly DATA=true && export DATA
      ;;
    *)
      if [[ $param == --* ]]; then
        error "Unknown parameter $param" 
      fi
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
