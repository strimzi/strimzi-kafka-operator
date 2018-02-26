#!/usr/bin/env bash

set -e

openssl aes-256-cbc -K $encrypted_c563b2a48eea_key -iv $encrypted_c563b2a48eea_iv -in .travis/github_deploy_key.enc -out github_deploy_key -d
chmod 600 github_deploy_key
eval `ssh-agent -s`
ssh-add github_deploy_key

git clone git@github.com:strimzi/strimzi.github.io.git /tmp/website
cp documentation/htmlnoheader/master.html /tmp/website/docs/master/master.html

pushd /tmp/website

if git diff --quiet; then
    echo "No changes to the output on this push; exiting."
    exit 0
fi

git config user.name "Travis CI"
git config user.email "ci@travis.tld"

git add docs/master/master.html
git commit -m "Update documentation (Travis CI build ${TRAVIS_BUILD_NUMBER})"
git push origin master

popd