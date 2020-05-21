#!/usr/bin/env bash

set -e

# Do update and install docker

apt update
apt install docker.io -y
systemctl unmask docker

mkdir /mnt/docker

sh -c "sed -i 's#ExecStart=/usr/bin/dockerd -H fd://#ExecStart=/usr/bin/dockerd -g /mnt/docker -H fd://#' /lib/systemd/system/docker.service"

systemctl daemon-reload
rsync -aqxP /var/lib/docker/ /mnt/docker

systemctl start docker