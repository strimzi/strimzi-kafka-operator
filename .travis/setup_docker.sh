#!/bin/bash

set -e

echo "===== SETUP DOCKER ===="
sudo apt-get -y update

sudo apt-get install -y apt-transport-https ca-certificates curl gnupg-agent software-properties-common
sudo mkdir -p /mnt/docker
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

sudo add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"

sudo apt-get -y update
sudo apt-get install docker-ce docker-ce-cli containerd.io

docker version