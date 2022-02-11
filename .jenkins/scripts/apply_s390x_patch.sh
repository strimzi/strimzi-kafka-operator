#!/usr/bin/env bash

sed -i 's#v1.7.0#ff3ee40293cb943c4ffb70d808c4e8772189c7e1#g' docker-images/kaniko-executor/Makefile

patch -p1 <.jenkins/scripts/kafka-rocksdbjni-s390x.patch
