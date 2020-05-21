#!/usr/bin/env bash

curl -L https://github.com/mikefarah/yq/releases/download/3.3.2/yq_linux_amd64 > yq && chmod +x yq
sudo cp yq /usr/bin/

sudo apt update
sudo apt install docker.io -y
sudo systemctl unmask docker

sudo mkdir /mnt/docker

sudo sh -c "sed -i 's#ExecStart=/usr/bin/dockerd -H fd://#ExecStart=/usr/bin/dockerd -g /mnt/docker -H fd://#' /lib/systemd/system/docker.service"

sudo systemctl daemon-reload
sudo rsync -aqxP /var/lib/docker/ /mnt/docker

sudo systemctl start docker
