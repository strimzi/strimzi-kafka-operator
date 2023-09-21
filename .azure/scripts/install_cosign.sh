#!/usr/bin/env bash

readonly VERSION="2.2.0"

ARCH=$1
if [ -z "$ARCH" ]; then
    ARCH="amd64"
fi

curl -L https://github.com/sigstore/cosign/releases/download/v${VERSION}/cosign-linux-${ARCH} > cosign && chmod +x cosign
sudo mv cosign /usr/bin/
