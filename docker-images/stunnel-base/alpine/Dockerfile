#
# License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
#
# Alpine images are an experimental feature not supported by the core Strimzi team
#
ARG ALPINE_VERSION=3.9

FROM alpine:$ALPINE_VERSION

# set Stunnel home folder
ARG STUNNEL_HOME=/opt/stunnel

RUN apk add --no-cache bash stunnel;

RUN mkdir -p $STUNNEL_HOME && mkdir -p -m g+rw /usr/local/var/run/

# copy scripts for Stunnel
COPY ./scripts/ $STUNNEL_HOME

WORKDIR $STUNNEL_HOME
