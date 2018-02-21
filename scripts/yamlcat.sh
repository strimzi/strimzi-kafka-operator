#!/usr/bin/env bash

echo "---"
while [ $# -gt 0 ]; do
  COMMAND=$1
  case $COMMAND in
    -name)
      DAEMON_NAME=$2
      CONSOLE_OUTPUT_FILE=$LOG_DIR/$DAEMON_NAME.out
      shift 2
      ;;
    -loggc)
      if [ -z "$KAFKA_GC_LOG_OPTS" ]; then
        GC_LOG_ENABLED="true"
      fi
      shift
      ;;
    -daemon)
      DAEMON_MODE="true"
      shift
      ;;
    *)
      if [ -f "$COMMAND" ]; then
        cat $COMMAND
        echo "" # in case file doesn't end with newline
        echo "---"
      fi
      shift
      ;;
  esac
done

