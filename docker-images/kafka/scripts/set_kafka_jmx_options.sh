#!/usr/bin/env bash
set -e

JMX_ENABLED="$1"
JMX_USERNAME="$2"
JMX_PASSWORD="$3"

echo "${JMX_ENABLED}, ${JMX_USERNAME}, ${JMX_PASSWORD}"

if [ "$JMX_ENABLED" = "true" ]; then
  export KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.rmi.port=9999 -Dcom.sun.management.jmxremote=true -Djava.rmi.server.hostname=$(hostname -i) -Djava.net.preferIPv4Stack=true"

  if [ -n "$JMX_USERNAME" ]; then
    # Secure JMX port on 9999 with username and password
    JMX_ACCESS_FILE="/tmp/access.file"
    JMX_PASSWORD_FILE="/tmp/password.file"

    cat << EOF > "${JMX_ACCESS_FILE}"
${JMX_USERNAME} readonly
EOF

    cat << EOF > "${JMX_PASSWORD_FILE}"
$JMX_USERNAME $JMX_PASSWORD
EOF
    chmod 400 "${JMX_PASSWORD_FILE}"
    export KAFKA_JMX_OPTS="${KAFKA_JMX_OPTS} -Dcom.sun.management.jmxremote.access.file=${JMX_ACCESS_FILE} -Dcom.sun.management.jmxremote.password.file=${JMX_PASSWORD_FILE}  -Dcom.sun.management.jmxremote.authenticate=true"
  else
    # expose the port insecurely
    export KAFKA_JMX_OPTS="${KAFKA_JMX_OPTS} -Dcom.sun.management.jmxremote.authenticate=false"
  fi
fi