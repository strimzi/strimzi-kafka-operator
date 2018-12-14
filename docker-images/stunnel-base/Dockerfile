FROM centos:7

RUN yum -y install stunnel net-tools && yum clean all -y

# set Stunnel home folder
ENV STUNNEL_HOME=/opt/stunnel

# Add stunnel user with UID 1001
# The user is in the group 0 to have access to the mounted volumes and storage
RUN useradd -r -m -u 1001 -g 0 stunnel

RUN mkdir $STUNNEL_HOME && mkdir -p -m g+rw /usr/local/var/run/

# copy scripts for Stunnel
COPY ./scripts/ $STUNNEL_HOME

WORKDIR $STUNNEL_HOME
