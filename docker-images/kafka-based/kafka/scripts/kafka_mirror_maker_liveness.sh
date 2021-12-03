#!/usr/bin/env bash
set -e

if [ -f /tmp/mirror-maker-alive ] ; then
  rm -f /tmp/mirror-maker-alive 2&> /dev/null
  exit 0
else
  exit 1
fi
