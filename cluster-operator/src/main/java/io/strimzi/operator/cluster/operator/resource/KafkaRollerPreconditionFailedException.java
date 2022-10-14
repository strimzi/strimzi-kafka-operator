package io.strimzi.operator.cluster.operator.resource;

public class KafkaRollerPreconditionFailedException extends RuntimeException {

    public KafkaRollerPreconditionFailedException(String s) {
        super(s);
    }
}
