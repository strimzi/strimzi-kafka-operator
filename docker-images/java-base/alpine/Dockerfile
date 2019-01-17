#
# License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
#
# Alpine images are an experimental feature not supported by the core Strimzi team
#
ARG JAVA_ORDINAL_VERSION=8
ARG ALPINE_VERSION=3.9

FROM azul/zulu-openjdk-alpine:$JAVA_ORDINAL_VERSION as repackage
ARG JAVA_ORDINAL_VERSION=8
ARG JAVA_MINIMAL=/opt/jre

# build modules distribution
RUN if [ $JAVA_ORDINAL_VERSION -le 8 ]; then mkdir -p /opt; ln -s $(dirname $(dirname $(readlink -f "$(which java)"))) ${JAVA_MINIMAL};\
else jlink --verbose --add-modules \
java.base,\
# java.beans.PropertyChangeEvent is used by log4j
java.desktop,\
java.instrument,\
java.logging,\
java.management,\
java.naming,\
java.net.http,\
# java.sql.Date is used by snakeyaml, from io.prometheus.jmx
java.sql,\
java.xml,\
jdk.crypto.cryptoki,\
jdk.crypto.ec,\
jdk.dynalink,\
# com.sun.net.httpserver.HttpHandler is used by io.prometheus.jmx.shaded.io.prometheus.jmx.JavaAgent
jdk.httpserver,\
jdk.jdi,\
jdk.jdwp.agent,\
jdk.management,\
jdk.management.agent,\
jdk.net,\
jdk.security.auth,\
jdk.security.jgss,\
# sun.misc.Unsafe is used by way too many packages
jdk.unsupported,\
jdk.xml.dom\
 --compress 2 --strip-debug --no-header-files --no-man-pages --vm=server --output "${JAVA_MINIMAL}";\
fi

# Second stage, copy minimal JRE distribution
FROM alpine:$ALPINE_VERSION
ARG JAVA_MINIMAL=/opt/jre

COPY --from=repackage "$JAVA_MINIMAL" "$JAVA_MINIMAL"

ENV PATH="$PATH:$JAVA_MINIMAL/bin"

# Add strimzi user with UID 1001
# The user is in the group 0 to have access to the mounted volumes and storage
RUN apk add --no-cache libstdc++ bash ca-certificates curl openssl;\
    adduser -u 1001 -D strimzi root

# Copy scripts for starting Java apps
COPY scripts/* /bin/

CMD ["java", "-version"]
