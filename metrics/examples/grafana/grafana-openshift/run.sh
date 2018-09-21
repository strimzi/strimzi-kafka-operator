#!/bin/bash

source /etc/sysconfig/grafana-server

[ -z "${DATAD} ] && DATAD=${DATA_DIR}
[ -z "${PLGND} ] && PLGND=${PLUGINS_DIR}

if [ ! -z "${GF_INSTALL_PLUGINS}" ]; then
  OLDIFS=$IFS
  IFS=','
  for plugin in ${GF_INSTALL_PLUGINS}; do
    grafana-cli --pluginsDir ${PLGND} plugins install ${plugin}
  done
  IFS=$OLDIFS
fi

/usr/sbin/grafana-server \
--config=${CONF_FILE} \
--pidfile=${PID_FILE} \
cfg:default.paths.logs=${LOG_DIR} \
cfg:default.paths.data=${DATAD} \
cfg:default.paths.plugins=${PLGND}
