#!/usr/bin/env bash
set -Eeuo pipefail
if [[ $(uname -s) == "Darwin" ]]; then
  shopt -s expand_aliases
  alias echo="gecho"; alias dirname="gdirname"; alias grep="ggrep"; alias readlink="greadlink"
  alias tar="gtar"; alias sed="gsed"; alias date="gdate"; alias wc="gwc"
fi

LD_KAFKA_BROKERS=0
LD_STORAGE_TYPE=""
LD_JBOD_DISKS=0
LD_OUT_PATH="/tmp/log-dump"
LD_NAMESPACE=""
LD_CLUSTER=""
LD_TOPIC=""
LD_TOPIC_CO="__consumer_offsets" && readonly LD_TOPIC_CO
LD_TOPIC_TS="__transaction_state" && readonly LD_TOPIC_TS
LD_TOPIC_CM="__cluster_metadata" && readonly LD_TOPIC_CM
LD_PARTITION=0
LD_SEGMENT=""
LD_GROUP_ID=""
LD_TXN_ID=""
LD_TOT_PART=50
LD_DRY_RUN=false
LD_DATA=false

error() {
  echo -e "$@" 1>&2 && exit 1
}

check_number() {
  local value="$1" regex='^[0-9]+$'
  if ! [[ $value =~ $regex ]]; then
    error "Not a number"
  fi
}

check_kube_conn() {
  kubectl version --output=yaml --request-timeout=10s 1>/dev/null
}

get_kafka_setup() {
  local kafka_yaml && kafka_yaml=$(kubectl -n "$LD_NAMESPACE" get kafka "$LD_CLUSTER" -o yaml ||true)
  LD_KAFKA_BROKERS=$(echo "$kafka_yaml" | yq eval ".spec.kafka.replicas" -)
  LD_STORAGE_TYPE=$(echo "$kafka_yaml" | yq eval ".spec.kafka.storage.type" -)
  LD_JBOD_DISKS=$(echo "$kafka_yaml" | yq eval ".spec.kafka.storage.volumes | length" -)
  if [[ -n $LD_KAFKA_BROKERS && $LD_KAFKA_BROKERS != "null" ]]; then
    echo "brokers: $LD_KAFKA_BROKERS, storage: $LD_STORAGE_TYPE, disks: $LD_JBOD_DISKS"
  else
    error "Kafka cluster $LD_CLUSTER not found in namespace $LD_NAMESPACE"
  fi
}

coord_partition() {
  local id="${1-}" part="${2-50}"
  if [[ -z $id ]]; then echo "Missing id parameter" && return; fi
  echo 'public void run(String id, int part) { System.out.println(abs(id.hashCode()) % part); }
    private int abs(int n) { return (n == Integer.MIN_VALUE) ? 0 : Math.abs(n); }
    run("'"$id"'", '"$part"');' | jshell -
}

find_segments() {
  local broker="$1" log_dir="$2"
  if [[ -z $broker || -z $log_dir ]]; then
    error "Missing parameters"
  fi
  local seg_files && seg_files=$(kubectl -n "$LD_NAMESPACE" exec "$LD_CLUSTER"-kafka-"$broker" -- \
    find "$log_dir" -printf "%f\n" 2>/dev/null | grep ".log")
  echo "$seg_files"
}

dump_segments() {
  local topic="$1" seg_files="$2"
  if [[ -z $seg_files || $(echo "$seg_files" | sed '/^\s*$/d' | wc -l) -eq 0 ]]; then
    echo "No segment found"
  fi
  local flags="--deep-iteration"
  if [[ $LD_DATA == true ]]; then
    flags="$flags --print-data-log"
  fi
  case "$topic" in
    "$LD_TOPIC_CO")
      flags="$flags --offsets-decoder"
      ;;
    "$LD_TOPIC_TS")
      flags="$flags --transaction-log-decoder"
      ;;
    "$LD_TOPIC_CM")
      flags="$flags --cluster-metadata-decoder"
      ;;
  esac
  flags="$flags --files"
  for seg_file in $seg_files; do
    if [[ -n $LD_SEGMENT && $LD_SEGMENT != "${seg_file%.log}" ]]; then
      continue
    fi
    echo "$seg_file"
    if [[ $LD_DRY_RUN == false ]]; then
      mkdir -p "$out_dir"
      kubectl -n "$LD_NAMESPACE" exec "$LD_CLUSTER"-kafka-"$i" -- \
        bash -c "cd $log_dir && /opt/kafka/bin/kafka-dump-log.sh $flags $seg_file" \
          > "$out_dir/$seg_file"
    fi
  done
}

dump_partition() {
  local topic="$1" partition="$2" broker="$3" disk="${4-}"
  if [[ -z $topic || -z $partition || -z $broker ]]; then
    error "Missing parameters"
  fi
  # context setup
  local disk_label="$topic-$partition segments in kafka-$broker"
  local log_dir="/var/lib/kafka/data/kafka-log$broker/$topic-$partition"
  local out_dir="$LD_OUT_PATH/$topic/kafka-$broker-$topic-$partition"
  if [[ -n $disk && $disk -ge 0 ]]; then
    disk_label="$topic-$partition segments in kafka-$broker-disk-$disk"
    log_dir="/var/lib/kafka/data-$disk/kafka-log$broker/$topic-$partition";
    out_dir="$LD_OUT_PATH/$topic/kafka-$broker-disk-$disk-$topic-$partition"
  fi
  # segment dump
  local seg_files && seg_files=$(find_segments "$broker" "$log_dir")
  echo "$disk_label"
  dump_segments "$topic" "$seg_files"
}

partition() {
  if [[ -z $LD_NAMESPACE || -z $LD_CLUSTER || -z $LD_TOPIC || -z $LD_PARTITION ]]; then
    error "Missing parameters"
  fi
  check_kube_conn
  get_kafka_setup
  # dump topic partition across the cluster (including replicas)
  for i in $(seq 0 $((LD_KAFKA_BROKERS-1))); do
    if [[ $LD_STORAGE_TYPE == "jbod" ]]; then
      for j in $(seq 0 $((LD_JBOD_DISKS-1))); do
        dump_partition "$LD_TOPIC" "$LD_PARTITION" "$i" "$j"
      done
    else
      dump_partition "$LD_TOPIC" "$LD_PARTITION" "$i"
    fi
  done
}

cg_offsets() {
  if [[ -z $LD_NAMESPACE || -z $LD_CLUSTER || -z $LD_GROUP_ID ]]; then
    error "Missing parameters"
  fi
  check_kube_conn
  get_kafka_setup
  # dump CG offsets coordinating partition across the cluster (including replicas)
  local partition && partition=$(coord_partition "$LD_GROUP_ID" "$LD_TOT_PART")
  echo "$LD_GROUP_ID coordinating partition: $partition"
  for i in $(seq 0 $((LD_KAFKA_BROKERS-1))); do
    if [[ $LD_STORAGE_TYPE == "jbod" ]]; then
      for j in $(seq 0 $((LD_JBOD_DISKS-1))); do
        dump_partition "$LD_TOPIC_CO" "$partition" "$i" "$j"
      done
    else
      dump_partition "$LD_TOPIC_CO" "$partition" "$i"
    fi
  done
}

txn_state() {
  if [[ -z $LD_NAMESPACE || -z $LD_CLUSTER || -z $LD_TXN_ID ]]; then
    error "Missing parameters"
  fi
  check_kube_conn
  get_kafka_setup
  # dump TX state coordinating partition across the cluster (including replicas)
  local partition && partition=$(coord_partition "$LD_TXN_ID" "$LD_TOT_PART")
  echo "$LD_TXN_ID coordinating partition: $partition"
  for i in $(seq 0 $((LD_KAFKA_BROKERS-1))); do
    if [[ $LD_STORAGE_TYPE == "jbod" ]]; then
      for j in $(seq 0 $((LD_JBOD_DISKS-1))); do
        dump_partition "$LD_TOPIC_TS" "$partition" "$i" "$j"
      done
    else
      dump_partition "$LD_TOPIC_TS" "$partition" "$i"
    fi
  done
}

cluster_meta() {
  if [[ -z $LD_NAMESPACE || -z $LD_CLUSTER ]]; then
    error "Missing parameters"
  fi
  check_kube_conn
  get_kafka_setup
  # dump cluster metadata partition across the cluster (including replicas)
  local partition="0"
  for i in $(seq 0 $((LD_KAFKA_BROKERS-1))); do
    if [[ $LD_STORAGE_TYPE == "jbod" ]]; then
      for j in $(seq 0 $((LD_JBOD_DISKS-1))); do
        dump_partition "$LD_TOPIC_CM" "$partition" "$i" "$j"
      done
    else
      dump_partition "$LD_TOPIC_CM" "$partition" "$i"
    fi
  done
}

readonly USAGE="
Usage: $0 [command] [params]

  partition       Dump topic partition
    -n, --namespace     Kubernetes namespace
    -c, --cluster       Kafka cluster name
    -t, --topic         Topic name
    -p, --partition     Partition number (zero-based)
    -s, --segment       Filter by segment name
    -O, --out-path      Output path (default: $LD_OUT_PATH)
    -D, --dry-run       Run without dumping (default: $LD_DRY_RUN)
    -A, --data          Include record payload (default: $LD_DATA)

  cg_offsets      Dump consumer group offsets partition by group.id
    -n, --namespace     Kubernetes namespace
    -c, --cluster       Kafka cluster name
    -G, --group-id      Consumer group id
    -T, --tot-part      Number of __consumer_offsets partitions (default: $LD_TOT_PART)
    -O, --out-path      Output path (default: $LD_OUT_PATH)
    -D, --dry-run       Run without dumping (default: $LD_DRY_RUN)

  txn_state       Dump transactions state partition by transactional.id
    -n, --namespace     Kubernetes namespace
    -c, --cluster       Kafka cluster name
    -X, --txn-id        Transactional id
    -T, --tot-part      Number of __transaction_state partitions (default: $LD_TOT_PART)
    -O, --out-path      Output path (default: $LD_OUT_PATH)
    -D, --dry-run       Run without dumping (default: $LD_DRY_RUN)

  cluster_meta    Dump cluster metadata partition (KRaft)
    -n, --namespace     Kubernetes namespace
    -c, --cluster       Kafka cluster name
    -O, --out-path      Output path (default: $LD_OUT_PATH)
    -D, --dry-run       Run without dumping (default: $LD_DRY_RUN)
"
readonly PARAMS=("$@")
i=0; for param in "${PARAMS[@]}"; do
  i=$((i+1))
  case $param in
    -n|--namespace)
      LD_NAMESPACE=${PARAMS[i]} && readonly LD_NAMESPACE
      ;;
    -c|--cluster)
      LD_CLUSTER=${PARAMS[i]} && readonly LD_CLUSTER
      ;;
    -t|--topic)
      LD_TOPIC=${PARAMS[i]} && readonly LD_TOPIC
      ;;
    -p|--partition)
      LD_PARTITION=${PARAMS[i]} && readonly LD_PARTITION
      check_number "$LD_PARTITION"
      ;;
    -s|--segment)
      LD_SEGMENT=${PARAMS[i]} && readonly LD_SEGMENT
      ;;
    -G|--group-id)
      LD_GROUP_ID=${PARAMS[i]} && readonly LD_GROUP_ID
      ;;
    -X|--txn-id)
      LD_TXN_ID=${PARAMS[i]} && readonly LD_TXN_ID
      ;;
    -T|--tot-part)
      LD_TOT_PART=${PARAMS[i]} && readonly LD_TOT_PART
      check_number "$LD_TOT_PART"
      ;;
    -O|--out-path)
      LD_OUT_PATH=${PARAMS[i]} && readonly LD_OUT_PATH
      ;;
    -D|--dry-run)
      LD_DRY_RUN=true && readonly LD_DRY_RUN
      ;;
    -A|--data)
      LD_DATA=true && readonly LD_DATA
      ;;
    *)
      if [[ $param == -* || $param == --* ]]; then
        error "Unknown parameter: $param"
      fi
      ;;
  esac
done
readonly COMMAND="${1-}"
if [[ -z "$COMMAND" ]]; then
  error "$USAGE"
else
  if (! declare -F "$COMMAND" >/dev/null); then
    error "Invalid command"
  fi
  eval "$COMMAND"
fi
