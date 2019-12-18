#!/usr/bin/env bash

# Remove moby-engine which use docker 3.0.8 on azure
sudo apt-get remove --purge iotedge
sudo apt-get remove --purge moby-cli
sudo apt-get remove --purge moby-engine

wget -O docker.deb https://download.docker.com/linux/ubuntu/dists/bionic/pool/stable/amd64/docker-ce_18.03.1~ce~3-0~ubuntu_amd64.deb
sudo dpkg -i ./docker.deb