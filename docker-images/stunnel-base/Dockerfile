FROM centos:7

RUN yum -y install stunnel && yum clean all -y

# set Stunnel home folder
ENV STUNNEL_HOME=/opt/stunnel

# Add stunnel group / user
RUN groupadd -r -g 1001 stunnel && useradd -r -m -u 1001 -g stunnel stunnel

RUN mkdir $STUNNEL_HOME && mkdir -p -m g+rw /usr/local/var/run/

WORKDIR $STUNNEL_HOME
