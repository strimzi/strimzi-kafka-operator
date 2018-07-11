FROM strimzi/stunnel-base:latest

# exposing Zookeeper ports
EXPOSE 2888 3888

# copy scripts for starting Stunnel
COPY ./scripts/ $STUNNEL_HOME

USER stunnel:stunnel

CMD ["/opt/stunnel/stunnel_run.sh"]
