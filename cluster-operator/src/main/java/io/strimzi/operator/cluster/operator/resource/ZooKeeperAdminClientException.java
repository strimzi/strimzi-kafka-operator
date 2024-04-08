/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

/**
 * Thrown for exceptional circumstances when constructing a ZooKeeper Admin Client.
 */
public class ZooKeeperAdminClientException extends RuntimeException {
    /**
     * Constructor
     *
     * @param message   Error message
     * @param cause     Exception which caused this error
     */
    public ZooKeeperAdminClientException(String message, Throwable cause) {
        super(message, cause);
    }
}