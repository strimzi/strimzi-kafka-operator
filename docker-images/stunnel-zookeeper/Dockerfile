FROM centos:7

RUN yum -y install stunnel && yum clean all -y 

# set Stunnel home folder
ENV STUNNEL_HOME=/opt/stunnel

# exposing Zookeeper ports
EXPOSE 2888 3888

# Create Stunnel home and place for the stunnel.pid file
RUN mkdir $STUNNEL_HOME && mkdir -p -m g+rw /usr/local/var/run/ 

# copy scripts for starting Stunnel
COPY ./scripts/ $STUNNEL_HOME

WORKDIR $STUNNEL_HOME

CMD ["/opt/stunnel/stunnel_run.sh"]
