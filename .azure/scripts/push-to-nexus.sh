#!/usr/bin/env bash
set -e

function cleanup() {
  rm -rf signing.gpg
  gpg --delete-keys
  gpg --delete-secret-keys
}

# Run the cleanup on failure / exit
trap cleanup EXIT

export GPG_TTY=$(tty)
echo $GPG_SIGNING_KEY | base64 -d > signing.gpg
gpg --batch --import signing.gpg

GPG_EXECUTABLE=gpg mvn $MVN_ARGS -DskipTests -s ./.azure/scripts/settings.xml -pl ./,crd-annotations,crd-generator,test,api -P ossrh deploy

cleanup