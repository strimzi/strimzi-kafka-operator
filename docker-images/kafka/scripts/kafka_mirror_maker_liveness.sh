#!/usr/bin/env bash
if [ -f /tmp/mirror-maker-alive ] ; then
  rm /tmp/mirror-maker-alive 2&> /dev/null
  exit 0
else
  exit 1
fi
