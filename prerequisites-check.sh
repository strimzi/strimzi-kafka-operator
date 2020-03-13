#!/usr/bin/env bash

RED="\033[0;31m"
NO_COLOUR="\033[0m"

function check_command_present() {
    command -v "${1}" >/dev/null 2>&1 || { echo -e >&2 "${RED}I require ${1} but it's not installed.  Aborting.${NO_COLOUR}"; exit 1; }
}

check_command_present yq
check_command_present mvn
check_command_present git
check_command_present docker

YQ_VERSION="$(yq --version | ${SED} 's/^.* //g')"
if [[ ${YQ_VERSION} != "3."* ]]; then
  echo -e "${RED}yq vesion is ${YQ_VERSION}, version must be 3.*${NO_COLOUR}"
  exit 1
fi
