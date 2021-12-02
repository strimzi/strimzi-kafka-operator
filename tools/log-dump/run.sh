#!/usr/bin/env bash
set -Eeuo pipefail
if [[ $(uname -s) == "Darwin" ]]; then
  shopt -s expand_aliases
  alias echo="gecho"; alias dirname="gdirname"; alias grep="ggrep"; alias readlink="greadlink"
  alias tar="gtar"; alias sed="gsed"; alias start_offsetrt="gstart_offsetrt"; alias date="gdate"; alias wc="gwc"
fi
readonly CO_TOPIC="__consumer_offsets"
readonly TS_TOPIC="__transaction_state"
readonly CM_TOPIC="__cluster_metadata"
KAFKA_BROKERS=0
STORAGE_TYPE=""
JBOD_DISKS=0

# user input
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
  KAFKA_BROKERS=$(kubectl -n $NAMESPACE get k $CLUSTER -o yaml 2>/dev/null | yq eval ".spec.kafka.replicas" -) ||true
  STORAGE_TYPE=$(kubectl -n $NAMESPACE get k $CLUSTER -o yaml 2>/dev/null | yq eval ".spec.kafka.storage.type" -) ||true
  JBOD_DISKS=$(kubectl -n $NAMESPACE get k $CLUSTER -o yaml 2>/dev/null | yq eval ".spec.kafka.storage.volumes | length" -) ||true
  if [[ $KAFKA_BROKERS != "null" ]]; then
    echo "brokers: $KAFKA_BROKERS, storage: $STORAGE_TYPE, disks: $JBOD_DISKS"
  else
    error "Kafka cluster $CLUSTER not found in namespace $NAMESPACE"
  fi
}

coord-partition() {
  local id="$1"
  local part="${2-$TOT_PART}"
  if [[ -n $id && -n $part ]]; then
    echo 'public void run(String id, int part) { System.out.println(abs(id.hashCode()) % part); }    
      private int abs(int n) { return (n == Integer.MIN_VALUE) ? 0 : Math.abs(n); } 
      run("'$id'", '$part');' \
      | jshell -
  fi
}

find_segments() {
  local broker="$1"
  local log_dir="$2"
  if [[ -n $broker && -n $log_dir ]]; then
    local seg_files=$(kubectl -n $NAMESPACE exec $CLUSTER-kafka-$broker -- \
      find $log_dir -printf "%f\n" 2>/dev/null | grep ".log")
    echo $seg_files
  else
    error "Missing required parameters"
  fi
}

dump_segments() {
  local seg_files="$1"
  if [[ -n $seg_files && $(echo "$seg_files" | sed '/^\s*$/d' | wc -l) -gt 0 ]]; then
    
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
    
    for seg_file in $(echo $seg_files); do
      if [[ -n $SEGMENT && $SEGMENT != "${seg_file%.log}" ]]; then
        continue
      fi
      echo $seg_file
      if [[ $DRY_RUN == false ]]; then
        mkdir -p $out_dir
        kubectl -n $NAMESPACE exec $CLUSTER-kafka-$i -- \
          ./bin/kafka-dump-log.sh $flags $log_dir/$seg_file > $out_dir/$seg_file
      fi
    done
    
  else
    echo "No segment found"
  fi
}

dump_partition() {
  local topic="$1"
  local partition="$2"
  local broker="$3"
  local disk="${4-}"
  if [[ -n $topic && -n $partition && -n $broker ]]; then
    
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
    local seg_files=$(find_segments $broker $log_dir)
    echo $disk_label
    dump_segments "$seg_files"
    
  else
    error "Missing required parameters"
  fi
}

partition() {
  if [[ -n $NAMESPACE && -n $CLUSTER && -n $TOPIC && -n $PARTITION ]]; then
    check_kube_conn
    get_kafka_setup
    
    # dump topic partition across the cluster (including replicas)
    for i in $(seq 0 $(($KAFKA_BROKERS-1))); do
      if [[ $STORAGE_TYPE == "jbod" ]]; then
        for j in $(seq 0 $(($JBOD_DISKS-1))); do
          dump_partition $TOPIC $PARTITION $i $j
        done
      else 
        dump_partition $TOPIC $PARTITION $i
      fi
    done
    
  else
    error "Missing required options"
  fi
}

cg_offsets() {
  if [[ -n $NAMESPACE && -n $CLUSTER && -n $GROUP_ID ]]; then
    check_kube_conn
    get_kafka_setup
          
    # dump CG offsets coordinating partition across the cluster (including replicas)
    local partition=$(coord-partition "$GROUP_ID" "$TOT_PART")
    echo "$GROUP_ID coordinating partition: $partition"
    for i in $(seq 0 $(($KAFKA_BROKERS-1))); do
      if [[ $STORAGE_TYPE == "jbod" ]]; then
        for j in $(seq 0 $(($JBOD_DISKS-1))); do
          dump_partition $CO_TOPIC $partition $i $j
        done
      else
        dump_partition $CO_TOPIC $partition $i
      fi
    done
    
  else
    error "Missing required options"
  fi
}

txn_state() {
  if [[ -n $NAMESPACE && -n $CLUSTER && -n $TXN_ID ]]; then
    check_kube_conn
    get_kafka_setup
        
    # dump TX state coordinating partition across the cluster (including replicas)
    local partition=$(coord-partition "$TXN_ID" "$TOT_PART")
    echo "$TXN_ID coordinating partition: $partition"
    for i in $(seq 0 $(($KAFKA_BROKERS-1))); do
      if [[ $STORAGE_TYPE == "jbod" ]]; then
        for j in $(seq 0 $(($JBOD_DISKS-1))); do
          dump_partition $TS_TOPIC $partition $i $j
        done
      else
        dump_partition $TS_TOPIC $partition $i
      fi
    done
    
  else
    error "Missing required options"
  fi
}

cluster_meta() {
  if [[ -n $NAMESPACE && -n $CLUSTER ]]; then
    check_kube_conn
    get_kafka_setup
        
    # dump cluster metadata partition across the cluster (including replicas)
    local partition="0"
    for i in $(seq 0 $(($KAFKA_BROKERS-1))); do
      if [[ $STORAGE_TYPE == "jbod" ]]; then
        for j in $(seq 0 $(($JBOD_DISKS-1))); do
          dump_partition $CM_TOPIC $partition $i $j
        done
      else
        dump_partition $CM_TOPIC $partition $i
      fi
    done

  else
    error "Missing required options"
  fi
}

readonly USAGE="
Usage: $0 [command] [params]

  partition       Dump partition
    --namespace     Kubernetes namespace
    --cluster       Kafka cluster name
    --topic         Data topic name
    --partition     Partition number (zero-based)
    --segment       Filter by segment name
    --out-path      Output path (default: $OUT_PATH)
    --dry-run       Run without dumping (default: $DRY_RUN)
    --data          Include record payload (default: $DATA)

  cg_offsets      Dump consumer group offsets by group.id
    --namespace     Kubernetes namespace
    --cluster       Kafka cluster name
    --group-id      Consumer group id
    --tot-part      Consumer offsets partitions (default: $TOT_PART)
    --out-path      Output path (default: $OUT_PATH)
  
  txn_state       Dump transactions state by transactional.id
    --namespace     Kubernetes namespace
    --cluster       Kafka cluster name
    --txn-id        Transactional id
    --tot-part      Transaction state partitions (default: $TOT_PART)
    --out-path      Output path (default: $OUT_PATH)
    
  cluster_meta    Dump cluster metadata (KRaft)
    --namespace     Kubernetes namespace
    --cluster       Kafka cluster name
    --out-path      Output path (default: $OUT_PATH)
"
readonly PARAMS="${@}"
readonly PARRAY=($PARAMS)
i=0
for param in $PARAMS; do
  i=$(($i+1))
  case $param in
    --namespace)
      export NAMESPACE=${PARRAY[i]}
      readonly NAMESPACE
      ;;
    --cluster)
      export CLUSTER=${PARRAY[i]}
      readonly CLUSTER
      ;;
    --topic)
      export TOPIC=${PARRAY[i]}
      readonly TOPIC
      ;;
    --partition)
      export PARTITION=${PARRAY[i]}
      readonly PARTITION
      check_number $PARTITION
      ;;
    --segment)
      export SEGMENT=${PARRAY[i]}
      readonly SEGMENT
      ;;
    --group-id)
      export GROUP_ID=${PARRAY[i]}
      readonly GROUP_ID
      ;;
    --txn-id)
      export TXN_ID=${PARRAY[i]}
      readonly TXN_ID
      ;;
    --tot-part)
      export TOT_PART=${PARRAY[i]}
      readonly TOT_PART
      check_number $TOT_PART
      ;;
    --out-path)
      export OUT_PATH=${PARRAY[i]}
      readonly OUT_PATH
      ;;
    --dry-run)
      export DRY_RUN=true
      readonly DRY_RUN
      ;;
    --data)
      export DATA=true
      readonly DATA
      ;;
    *)
      if [[ $param == --* ]]; then
        error "Unknown parameter $param" 
      fi
      ;;
  esac
done
readonly COMMAND="${1-}"
case "$COMMAND" in
  partition)
    partition
    ;;
  cg_offsets)
    cg_offsets
    ;;
  txn_state)
    txn_state
    ;;
  cluster_meta)
    cluster_meta
    ;;
  *)
    error "$USAGE"
    ;;
esac
