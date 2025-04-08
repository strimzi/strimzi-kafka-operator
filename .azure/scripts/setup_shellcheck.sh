#!/usr/bin/env bash
set -xe

ARCH=$1
if [ -z "$ARCH" ] || [ "$ARCH" == "amd64" ]; then
    ARCH="x86_64"
fi

if [ "$ARCH" == "arm64" ]; then
    ARCH="aarch64"
fi

ARCH=$1
if [ -z "$ARCH" ] || [ "$ARCH" == "amd64" ]; then
    ARCH="x86_64"
fi

if [ "$ARCH" == "arm64" ]; then
    ARCH="aarch64"
fi

readonly VERSION="0.9.0"
wget https://github.com/koalaman/shellcheck/releases/download/v$VERSION/shellcheck-v$VERSION.linux.$ARCH.tar.xz -O shellcheck.tar.xz
tar xf shellcheck.tar.xz -C /tmp --strip-components 1
chmod +x /tmp/shellcheck
sudo mv /tmp/shellcheck /usr/bin
