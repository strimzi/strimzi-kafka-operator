#!/usr/bin/env bash
set -Eeuo pipefail
if [[ $(uname -s) == "Darwin" ]]; then
  shopt -s expand_aliases
  alias echo="gecho"; alias dirname="gdirname"; alias grep="ggrep"; alias readlink="greadlink"
  alias tar="gtar"; alias sed="gsed"; alias start_offsetrt="gstart_offsetrt"; alias date="gdate"; alias wc="gwc"
fi
__HOME="" && pushd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" >/dev/null \
  && { __HOME=$PWD; popd >/dev/null; } && readonly __HOME
__KAFKA_BROKERS=0
__STORAGE_TYPE=""
__JBOD_DISKS=0

COMMAND=""
OUT_PATH="/tmp/klog-dump"
NAMESPACE=""
CLUSTER=""
TOPIC=""
PARTITION=0
SEGMENT=""
GROUP_ID=""
TXN_ID=""
NUM_PART=50
DRY_RUN=false
DATA=false

__error() {
    echo "$@" 1>&2
    exit 1
}

__check_number() {
  local value="$1"
  local regex='^[0-9]+$'
  if ! [[ $value =~ $regex ]]; then
     __error "Not a number"
  fi
}

__check_kube_conn() {
    kubectl version --request-timeout=10s 1>/dev/null
}

__get_kafka_setup() {
  __KAFKA_BROKERS=$(kubectl -n $NAMESPACE get k $CLUSTER -o yaml 2>/dev/null | yq eval ".spec.kafka.replicas" -) ||true
  __STORAGE_TYPE=$(kubectl -n $NAMESPACE get k $CLUSTER -o yaml 2>/dev/null | yq eval ".spec.kafka.storage.type" -) ||true
  __JBOD_DISKS=$(kubectl -n $NAMESPACE get k $CLUSTER -o yaml 2>/dev/null | yq eval ".spec.kafka.storage.volumes | length" -) ||true
  if [[ $__KAFKA_BROKERS != "null" ]]; then
    echo "brokers: $__KAFKA_BROKERS, storage: $__STORAGE_TYPE, disks: $__JBOD_DISKS"
  else
    __error "Kafka cluster $CLUSTER not found in namespace $NAMESPACE"
  fi
}

__dump_part_segments() {
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
    local flags="--deep-iteration"
    if [[ $topic == "__transaction_state" ]]; then
      flags="--transaction-log-decoder"
    fi
    if [[ $DATA == true ]]; then
      flags="$flags --print-data-log"
    fi
    flags="$flags --files"
    
    # find segments
    local seg_files=$(kubectl -n $NAMESPACE exec $CLUSTER-kafka-$broker -- find $log_dir -printf "%f\n" 2>/dev/null | grep ".log")
    local seg_num=$(echo "$seg_files" | sed '/^\s*$/d' | wc -l)
    
    # dump segments
    echo $disk_label
    if [[ $seg_num -gt 0 ]]; then
      for seg_file in $(echo $seg_files); do
        if [[ -n $SEGMENT && $SEGMENT != "${seg_file%.log}" ]]; then
          continue
        fi
        echo $seg_file
        if [[ $DRY_RUN == false ]]; then
          mkdir -p $out_dir
          kubectl -n $NAMESPACE exec $CLUSTER-kafka-$i -- ./bin/kafka-dump-log.sh $flags $log_dir/$seg_file > $out_dir/$seg_file
        fi
      done
    else
      echo "No segment found"
    fi
    
  else
    __error "Missing required parameters"
  fi
}

partition() {
  if [[ -n $NAMESPACE && -n $CLUSTER && -n $TOPIC && -n $PARTITION ]]; then
    __check_kube_conn
    __get_kafka_setup
    
    # dump topic partition across the cluster (including replicas)
    for i in $(seq 0 $(($__KAFKA_BROKERS-1))); do
      if [[ $__STORAGE_TYPE == "jbod" ]]; then
        for j in $(seq 0 $(($__JBOD_DISKS-1))); do
          __dump_part_segments $TOPIC $PARTITION $i $j
        done
      else 
        __dump_part_segments $TOPIC $PARTITION $i
      fi
    done
    
  else
    __error "Missing required options"
  fi
}

group_offsets() {
  if [[ -n $NAMESPACE && -n $CLUSTER && -n $GROUP_ID ]]; then
      __check_kube_conn
      __get_kafka_setup
          
      # dump __consumer_offsets coordinating partition across the cluster (including replicas)
      local group_part=$(klog group-coordinating-partition $GROUP_ID num-partitions=$NUM_PART)
      echo "$GROUP_ID coordinating partition: $group_part"
      for i in $(seq 0 $(($__KAFKA_BROKERS-1))); do
        if [[ $__STORAGE_TYPE == "jbod" ]]; then
          for j in $(seq 0 $(($__JBOD_DISKS-1))); do
            __dump_part_segments "__consumer_offsets" $group_part $i $j
          done
        else
          __dump_part_segments "__consumer_offsets" $group_part $i
        fi
      done
    
    else
      __error "Missing required options"
  fi
}

txn_state() {
  if [[ -n $NAMESPACE && -n $CLUSTER && -n $TXN_ID ]]; then
    __check_kube_conn
    __get_kafka_setup
        
    # dump __transaction_state coordinating partition across the cluster (including replicas)
    local txn_part=$(klog txn-coordinating-partition $TXN_ID num-partitions=$NUM_PART)
    echo "$TXN_ID coordinating partition: $txn_part"
    for i in $(seq 0 $(($__KAFKA_BROKERS-1))); do
      if [[ $__STORAGE_TYPE == "jbod" ]]; then
        for j in $(seq 0 $(($__JBOD_DISKS-1))); do
          __dump_part_segments "__transaction_state" $txn_part $i $j
        done
      else
        __dump_part_segments "__transaction_state" $txn_part $i
      fi
    done

  else
    __error "Missing required options"
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

  group_offsets   Dump offsets by group.id
    --namespace     Kubernetes namespace
    --cluster       Kafka cluster name
    --group-id      Consumer group id
    --num-part      Consumer offsets partitions (default: $NUM_PART)
    --out-path      Output path (default: $OUT_PATH)
  
  txn_state       Dump txn state by transactional.id
    --namespace     Kubernetes namespace
    --cluster       Kafka cluster name
    --txn-id        Transactional id
    --num-part      Transaction state partitions (default: $NUM_PART)
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
            __check_number $PARTITION
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
        --num-part)
            export NUM_PART=${PARRAY[i]}
            readonly NUM_PART
            __check_number $NUM_PART
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
    esac
done
readonly COMMAND="${1-}"
case "$COMMAND" in
    partition)
        partition
        ;;
    group_offsets)
        group_offsets
        ;;
    txn_state)
        txn_state
        ;;
    *)
        __error "$USAGE"
        ;;
esac
