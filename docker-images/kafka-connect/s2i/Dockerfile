FROM strimzi/kafka-connect:latest

USER root:root

# Set labels used in OpenShift to describe the builder images
LABEL io.openshift.s2i.scripts-url=image:///opt/s2i \
      io.openshift.tags="builder"

# Set S2I folder
ENV S2I_HOME=/opt/s2i

# Copy S2I scripts
COPY ./scripts/ $S2I_HOME

USER 1001:1001

CMD ["/opt/s2i/run"]