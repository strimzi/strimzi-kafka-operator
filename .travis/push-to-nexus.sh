#!/usr/bin/env bash

openssl aes-256-cbc -K $encrypted_57e4d0a26fa7_key -iv $encrypted_57e4d0a26fa7_iv -in signing.gpg.enc -out signing.gpg -d
gpg --import signing.gpg

export GPG_EXECUTABLE=gpg

pushd ../api
mvn -DskipTests -s ../.travis/settings.xml clean package gpg:sign deploy -P ossrh
popd

rm -rf signing.gpg
gpg --delete-keys
gpg --delete-secret-keys