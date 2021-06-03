#!/usr/bin/env bash

curl -kL https://repo.phenix.carrefour.com/common/yq/yq.v4.2.1 > yq && chmod +x yq
sudo cp yq /usr/bin/
