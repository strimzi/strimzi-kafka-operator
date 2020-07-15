#!/usr/bin/env bash

set -e

# Remove moby-engine which use docker 3.0.8 on azure
sudo apt-get remove --purge iotedge
sudo apt-get remove --purge moby-cli
sudo apt-get remove --purge moby-engine
sudo apt-get remove --purge containerd* docker.io
sudo apt-get install containerd.io

# Do update and install docker
sudo apt update
sudo apt install docker.io
sudo systemctl unmask docker

sudo mkdir /mnt/docker

sudo sh -c "sed -i 's#ExecStart=/usr/bin/dockerd -H fd://#ExecStart=/usr/bin/dockerd -g /mnt/docker -H fd://#' /lib/systemd/system/docker.service"

sudo systemctl daemon-reload
sudo rsync -aqxP /var/lib/docker/ /mnt/docker

sudo systemctl start docker
