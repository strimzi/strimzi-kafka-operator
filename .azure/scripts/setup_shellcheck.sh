#!/usr/bin/env bash
set -e

readonly VERSION="0.9.0"
wget https://github.com/koalaman/shellcheck/releases/download/v$VERSION/shellcheck-v$VERSION.linux.x86_64.tar.xz -O shellcheck.tar.xz
tar xf shellcheck.tar.xz -C /tmp --strip-components 1
chmod +x /tmp/shellcheck
sudo mv /tmp/shellcheck /usr/bin
