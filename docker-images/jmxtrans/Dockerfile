FROM jmxtrans/jmxtrans
RUN chown -R jmxtrans "/usr/share/jmxtrans"
USER noRoot
COPY docker-entrypoint.sh /docker-entrypoint.sh
COPY jmxtrans_liveness_check.sh /opt/jmx/
ENTRYPOINT [ "/docker-entrypoint.sh",  "start-without-jmx" ]