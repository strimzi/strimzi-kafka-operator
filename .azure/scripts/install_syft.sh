#!/usr/bin/env bash
set -e

readonly VERSION="0.90.0"

ARCH=$1
if [ -z "$ARCH" ]; then
    ARCH="amd64"
fi

wget https://github.com/anchore/syft/releases/download/v${VERSION}/syft_${VERSION}_linux_${ARCH}.tar.gz -O syft.tar.gz
tar xf syft.tar.gz -C /tmp
chmod +x /tmp/syft
sudo mv /tmp/syft /usr/bin
