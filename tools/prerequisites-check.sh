#!/usr/bin/env bash
set -e

# shellcheck source=./multi-platform-support.sh
source "$(dirname "$(realpath "${BASH_SOURCE[0]}")")"/../tools/multi-platform-support.sh

RED="\033[0;31m"
NO_COLOUR="\033[0m"

function check_command_present() {
    command -v "${1}" >/dev/null 2>&1 || { echo -e >&2 "${RED}I require ${1} but it's not installed.  Aborting.${NO_COLOUR}"; exit 1; }
}

check_command_present yq
check_command_present mvn
check_command_present git
check_command_present "${DOCKER_CMD:-docker}"
check_command_present shellcheck

# After version 3.3.1, yq --version sends the string to STDERR instead of STDOUT
YQ_VERSION="$(yq --version 2>&1 | ${SED} -r 's/^.* v?//g')"

IFS="." read -r -a YQ_ARRAY <<< "${YQ_VERSION#v}"

yq_err_msg="${RED}yq version is ${YQ_VERSION}, version must be 4.2.1 or above. Please download the latest version from https://github.com/mikefarah/yq/releases${NO_COLOUR}"

if [[ ${YQ_ARRAY[0]} -lt 4  ]]; then
  echo -e "$yq_err_msg"
  exit 1
fi

if [[ ${YQ_ARRAY[0]} -eq 4  ]] && [[ ${YQ_ARRAY[1]} -lt 2 ]]; then
  echo -e "$yq_err_msg"
  exit 1
fi

if [[ ${YQ_ARRAY[0]} -eq 4  ]] && [[ ${YQ_ARRAY[1]} -eq 2 ]] && [[ ${YQ_ARRAY[2]} -lt 1 ]]; then
  echo -e "$yq_err_msg"
  exit 1
fi
