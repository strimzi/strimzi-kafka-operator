#!/usr/bin/env bash

set -e

echo $GITHUB_DEPLOY_KEY | base64 -d > github_deploy_key
chmod 600 github_deploy_key
eval `ssh-agent -s`
ssh-add github_deploy_key

git clone git@github.com:strimzi/strimzi.github.io.git /tmp/website

# Operator docs
rm -rf  /tmp/website/docs/operators/in-development/images
rm -rf  /tmp/website/docs/operators/in-development/full/images
cp -vrL documentation/htmlnoheader/*                        /tmp/website/docs/operators/in-development
cp -vrL documentation/html/*                                /tmp/website/docs/operators/in-development/full

# Contributing Guide
rm -rf  /tmp/website/contributing/guide/images
cp -v   documentation/htmlnoheader/contributing-book.html   /tmp/website/contributing/guide/contributing.html
cp -v   documentation/html/contributing.html                /tmp/website/contributing/guide/full.html
cp -vrL documentation/htmlnoheader/images                   /tmp/website/contributing/guide/images

pushd /tmp/website

if [[ -z $(git status -s) ]]; then
    echo "No changes to the output on this push; exiting."
    exit 0
fi

git config user.name "Strimzi CI"
git config user.email "ci@strimzi.io"

git add -A
git commit -s -m "Update documentation (Strimzi CI build ${TRAVIS_BUILD_NUMBER})" --allow-empty
git push origin main

popd
