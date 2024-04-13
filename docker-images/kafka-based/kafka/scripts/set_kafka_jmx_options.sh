#!/usr/bin/env bash
set -e

JMX_ENABLED="$1"
JMX_USERNAME="$2"
JMX_PASSWORD="$3"

if [ "$JMX_ENABLED" = "true" ]; then
  KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.rmi.port=9999 -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=$(hostname -I | awk '{print $1}') -Djava.net.preferIPv4Stack=true"

  if [ -n "$JMX_USERNAME" ]; then
    mkdir -p /tmp/jmx/

    # Secure JMX port on 9999 with username and password
    JMX_ACCESS_FILE="/tmp/jmx/access.file"
    rm -f "${JMX_ACCESS_FILE}"
    JMX_PASSWORD_FILE="/tmp/jmx/password.file"
    rm -f "${JMX_PASSWORD_FILE}"

    cat << EOF > "${JMX_ACCESS_FILE}"
${JMX_USERNAME} readonly
EOF

    cat << EOF > "${JMX_PASSWORD_FILE}"
${JMX_USERNAME} ${JMX_PASSWORD}
EOF
    chmod 400 "${JMX_PASSWORD_FILE}"
    KAFKA_JMX_OPTS="${KAFKA_JMX_OPTS} -Dcom.sun.management.jmxremote.access.file=${JMX_ACCESS_FILE} -Dcom.sun.management.jmxremote.password.file=${JMX_PASSWORD_FILE}  -Dcom.sun.management.jmxremote.authenticate=true"
  else
    # expose the port insecurely
    KAFKA_JMX_OPTS="${KAFKA_JMX_OPTS} -Dcom.sun.management.jmxremote.authenticate=false"
  fi

  echo "Enabling JMX access ${KAFKA_JMX_OPTS}"
  export KAFKA_JMX_OPTS
fi
