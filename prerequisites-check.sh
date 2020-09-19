#!/usr/bin/env bash
set -e

source multi-platform-support.sh

RED="\033[0;31m"
NO_COLOUR="\033[0m"

function check_command_present() {
    command -v "${1}" >/dev/null 2>&1 || { echo -e >&2 "${RED}I require ${1} but it's not installed.  Aborting.${NO_COLOUR}"; exit 1; }
}

check_command_present yq
check_command_present mvn
check_command_present git
check_command_present docker

# After version 3.3.1, yq --version sends the string to STDERR instead of STDOUT
YQ_VERSION="$(yq --version 2>&1 | ${SED} 's/^.* //g')"

yq_array=($(echo "$YQ_VERSION" | tr '.' '\n'))

yq_err_msg="${RED}yq version is ${YQ_VERSION}, version must be 3.3.1 or above. Please download the latest version from https://github.com/mikefarah/yq/releases${NO_COLOUR}"

if [[ yq_array[0] -lt 3  ]]; then
  echo -e $yq_err_msg
  exit 1
elif [[ yq_array[1] -lt 3 ]]; then
  echo -e $yq_err_msg
  exit 1
elif [[ yq_array[2] -lt 1 ]]; then
  echo -e $yq_err_msg
  exit 1 
fi
