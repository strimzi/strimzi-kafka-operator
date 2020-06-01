#!/usr/bin/env bash
set -e

netstat -ntl | grep -q :"$1"