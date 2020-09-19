#!/usr/bin/env bash
set -e

wget https://github.com/koalaman/shellcheck/releases/download/v0.7.1/shellcheck-v0.7.1.linux.x86_64.tar.xz -O shellcheck.tar.xz
tar xf shellcheck.tar.xz -C /tmp --strip-components 1
chmod +x /tmp/shellcheck
sudo mv /tmp/shellcheck /usr/bin