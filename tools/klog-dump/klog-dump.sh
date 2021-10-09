#!/usr/bin/env bash
set -Eeuo pipefail
if [[ $(uname -s) == "Darwin" ]]; then
  shopt -s expand_aliases
  alias echo="gecho"; alias dirname="gdirname"; alias grep="ggrep"; alias readlink="greadlink"
  alias tar="gtar"; alias sed="gsed"; alias start_offsetrt="gstart_offsetrt"; alias date="gdate"; alias wc="gwc"
fi
__HOME="" && pushd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" >/dev/null \
  && { __HOME=$PWD; popd >/dev/null; } && readonly __HOME

COMMAND=""
OUT_PATH="/tmp/klog-dump"
NAMESPACE=""
CLUSTER=""
TOPIC=""
PARTITION=0
START_OFFSET=""
TRANS_ID=""
GROUP_ID=""
DRY=false
DATA=false

__KAFKA_BROKERS=0
__STORAGE_TYPE=""
__JBOD_DISKS=0

__error() {
    echo "$@" 1>&2
    exit 1
}

__check_kube_conn() {
    kubectl version --request-timeout=10s 1>/dev/null
}

__get_kafka_setup() {
  __KAFKA_BROKERS=$(kubectl -n $NAMESPACE get k $CLUSTER -o yaml 2>/dev/null | yq eval ".spec.kafka.replicas" -)
  __STORAGE_TYPE=$(kubectl -n $NAMESPACE get k $CLUSTER -o yaml 2>/dev/null | yq eval ".spec.kafka.storage.type" -)
  __JBOD_DISKS=$(kubectl -n $NAMESPACE get k $CLUSTER -o yaml 2>/dev/null | yq eval ".spec.kafka.storage.volumes | length" -)
  if [[ $__KAFKA_BROKERS != "null" ]]; then
    echo "brokers: $__KAFKA_BROKERS, storage: $__STORAGE_TYPE, disks: $__JBOD_DISKS"
  else
    __error "Kafka cluster not found"
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
        if [[ -n $START_OFFSET && $START_OFFSET != "${seg_file%.log}" ]]; then
          continue
        fi
        echo $seg_file
        if [[ $DRY == false ]]; then
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

offsets() {
  if [[ -n $NAMESPACE && -n $CLUSTER && -n $GROUP_ID ]]; then
      __check_kube_conn
      __get_kafka_setup
          
      # dump __consumer_offsets coordinating partition across the cluster (including replicas)
      local group_part=$(klog group-coordinating-partition $GROUP_ID)
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

tx_state() {
  if [[ -n $NAMESPACE && -n $CLUSTER && -n $TRANS_ID ]]; then
    __check_kube_conn
    __get_kafka_setup
        
    # dump __transaction_state coordinating partition across the cluster (including replicas)
    local trans_part=$(klog txn-coordinating-partition $TRANS_ID)
    echo "$TRANS_ID coordinating partition: $trans_part"
    for i in $(seq 0 $(($__KAFKA_BROKERS-1))); do
      if [[ $__STORAGE_TYPE == "jbod" ]]; then
        for j in $(seq 0 $(($__JBOD_DISKS-1))); do
          __dump_part_segments "__transaction_state" $trans_part $i $j
        done
      else
        __dump_part_segments "__transaction_state" $trans_part $i
      fi
    done

  else
    __error "Missing required options"
  fi
}

readonly USAGE="
Usage: $0 [commands] [options]

Commands:
  partition       Partition dump
  offsets         Offsets dump
  tx_state        Transaction state dump

Options:
  --namespace     Kubernetes namespace
  --cluster       Kafka cluster name
  --topic         Data topic name
  --partition     Partition number (zero-based)
  --start-offset  Start offset (segment name)
  --group-id      Consumer group ID
  --trans-id      Transactional ID
  --out-path      Output directory
  --dry-run       Run without dumping
  --data          Include data

Examples:
  # Print partition segments across the cluster
  $0 partition --namespace test --cluster my-cluster \\
    --topic my-topic --partition 0 --dry-run
    
  # Dump partition segments across the cluster
  $0 partition --namespace test --cluster my-cluster \\
    --topic my-topic --partition 0 --out-path ~/Downloads/my-dump
    
  # Dump partition segments across the cluster including data
  $0 partition --namespace test --cluster my-cluster \\
    --topic my-topic --partition 0 --data --out-path ~/Downloads/my-dump

  # Dump consumer group offsets segments
  $0 offsets --namespace test --cluster my-cluster \\
    --group-id my-group --out-path ~/Downloads/my-dump
  
  # Dump application transaction state segments
  $0 tx_state --namespace test --cluster my-cluster \\
    --trans-id my-trans --out-path ~/Downloads/my-dump
"
readonly OPTIONS="${@}"
readonly ARGUMENTS=($OPTIONS)
i=0
for argument in $OPTIONS; do
    i=$(($i+1))
    case $argument in
        --namespace)
            export NAMESPACE=${ARGUMENTS[i]}
            readonly NAMESPACE
            ;;
        --cluster)
            export CLUSTER=${ARGUMENTS[i]}
            readonly CLUSTER
            ;;
        --topic)
            export TOPIC=${ARGUMENTS[i]}
            readonly TOPIC
            ;;
        --partition)
            export PARTITION=${ARGUMENTS[i]}
            readonly PARTITION
            ;;
        --start-offset)
            export START_OFFSET=${ARGUMENTS[i]}
            readonly START_OFFSET
            ;;
        --group-id)
            export GROUP_ID=${ARGUMENTS[i]}
            readonly GROUP_ID
            ;;
        --trans-id)
            export TRANS_ID=${ARGUMENTS[i]}
            readonly TRANS_ID
            ;;
        --out-path)
            export OUT_PATH=${ARGUMENTS[i]}
            readonly OUT_PATH
            ;;
        --dry-run)
            export DRY=true
            readonly DRY
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
    offsets)
        offsets
        ;;
    tx_state)
        tx_state
        ;;
    *)
        __error "$USAGE"
        ;;
esac
