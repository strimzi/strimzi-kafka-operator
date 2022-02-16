#!/usr/bin/env bash

ARCH=$1

curl -L https://github.com/mikefarah/yq/releases/download/v4.6.3/yq_linux_${ARCH} > yq && chmod +x yq
sudo cp yq /usr/bin/
