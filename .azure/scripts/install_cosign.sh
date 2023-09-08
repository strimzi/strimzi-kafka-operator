#!/usr/bin/env bash

ARCH=$1
if [ -z "$ARCH" ]; then
    ARCH="amd64"
fi

curl -L https://github.com/sigstore/cosign/releases/download/v2.2.0/cosign-linux-${ARCH} > cosign && chmod +x cosign
sudo mv cosign /usr/bin/
