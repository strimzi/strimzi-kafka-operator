#!/usr/bin/env bash

sudo apt update
sudo apt install docker.io -y
sudo systemctl unmask docker

sudo mkdir /mnt/docker

sudo sh -c "sed -i 's#ExecStart=/usr/bin/dockerd -H fd://#ExecStart=/usr/bin/dockerd -g /mnt/docker -H fd://#' /lib/systemd/system/docker.service"

sudo systemctl daemon-reload
sudo rsync -aqxP /var/lib/docker/ /mnt/docker

sudo systemctl start docker