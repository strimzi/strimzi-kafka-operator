#!/usr/bin/env bash

# --fail MESSAGE
# --verify-success PARTITION
# --verify-fail PARTITION
# --verify-in-progress PARTITION
# --generate-current JSON
# --generate-propose JSON
# --execute-running
# --execute-started
# --execute-fail STR

echo "Random logging-like rubbish"

while [ $# -gt 0 ]; do
  COMMAND=$1
  case $COMMAND in
    --fail)
      echo "Partitions reassignment failed due to $2"
      shift 2
      ;;
     --generate-current)
      echo "Current partition replica assignment"
      echo "$2"
      echo ""
      shift 2
      ;;
    --generate-propose)
      echo "Proposed partition reassignment configuration"
      echo "$2"
      shift 2
      ;;
    --execute-running)
      echo "There is an existing assignment running."
      shift
      ;;
    --execute-started)
      echo "Successfully started reassignment of partitions."
      shift
      ;;
    --execute-fail)
      echo "Failed to reassign partitions $s"
      shift 2
      ;;
    --verify-success)
      echo "Reassignment of partition $2 completed successfully"
      shift 2
      ;;
    --verify-fail)
      echo "Reassignment of partition $2 failed"
      shift 2
      ;;
    --verify-in-progress)
      echo "Reassignment of partition $2 is still in progress"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

