#!/bin/sh
set -e

SCRIPT_DIR=$(dirname $0)
SCRIPTS_DIR=${SCRIPT_DIR}/scripts
SCRIPTS=(${SCRIPTS_DIR}/*)

# Copies all scripts into /bin directory
for ((i = 0; i < ${#SCRIPTS[@]}; ++i)); do
  cp -p "${SCRIPTS[$i]}" /bin

  # Set proper permissions
  script[i]="/bin/$(basename ${SCRIPTS[$i]})"
  chmod ug+x "${script[$i]}"
  chown $USER:root "${script[$i]}"
done
