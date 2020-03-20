/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

/**
 * Thrown for exceptional circumstances when scaling Zookeeper clusters up or down fails.
 */
public class ZookeeperScalingException extends RuntimeException {
    public ZookeeperScalingException() {
        super();
    }
    public ZookeeperScalingException(String s) {
        super(s);
    }
    public ZookeeperScalingException(Throwable cause) {
        super(cause);
    }
    public ZookeeperScalingException(String message, Throwable cause) {
        super(message, cause);
    }
}