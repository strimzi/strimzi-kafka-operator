#!/usr/bin/env bash

curl -L https://github.com/mikefarah/yq/releases/download/v4.2.1/yq_linux_amd64 > yq && chmod +x yq
sudo cp yq /usr/bin/
