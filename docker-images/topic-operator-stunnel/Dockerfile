FROM strimzi/stunnel-base:latest

# copy scripts for starting Stunnel
COPY ./scripts/ $STUNNEL_HOME

USER stunnel:stunnel

CMD ["/opt/stunnel/stunnel_run.sh"]
