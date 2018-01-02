FROM enmasseproject/java-base:8-3

ARG version=latest
ENV VERSION ${version}
ADD target/topic-controller.jar /

CMD ["/bin/launch_java.sh", "/topic-controller.jar"]