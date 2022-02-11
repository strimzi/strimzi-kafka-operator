#!/usr/bin/env bash

curl -L https://github.com/mikefarah/yq/releases/download/v4.6.3/yq_linux_s390x > yq && chmod +x yq
sudo cp yq /usr/bin/
