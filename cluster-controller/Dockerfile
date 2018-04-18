FROM strimzi/java-base:8-3

ADD target/cluster-controller.jar /

CMD ["/bin/launch_java.sh", "/cluster-controller.jar"]