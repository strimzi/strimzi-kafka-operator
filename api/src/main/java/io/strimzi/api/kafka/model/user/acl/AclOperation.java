/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.user.acl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum AclOperation {
    READ,
    WRITE,
    CREATE,
    DELETE,
    ALTER,
    DESCRIBE,
    CLUSTERACTION,
    ALTERCONFIGS,
    DESCRIBECONFIGS,
    IDEMPOTENTWRITE,
    ALL;

    @JsonCreator
    public static AclOperation forValue(String value) {
        switch (value) {
            case "Read":
                return READ;
            case "Write":
                return WRITE;
            case "Create":
                return CREATE;
            case "Delete":
                return DELETE;
            case "Alter":
                return ALTER;
            case "Describe":
                return DESCRIBE;
            case "ClusterAction":
                return CLUSTERACTION;
            case "AlterConfigs":
                return ALTERCONFIGS;
            case "DescribeConfigs":
                return DESCRIBECONFIGS;
            case "IdempotentWrite":
                return IDEMPOTENTWRITE;
            case "All":
                return ALL;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case READ:
                return "Read";
            case WRITE:
                return "Write";
            case CREATE:
                return "Create";
            case DELETE:
                return "Delete";
            case ALTER:
                return "Alter";
            case DESCRIBE:
                return "Describe";
            case CLUSTERACTION:
                return "ClusterAction";
            case ALTERCONFIGS:
                return "AlterConfigs";
            case DESCRIBECONFIGS:
                return "DescribeConfigs";
            case IDEMPOTENTWRITE:
                return "IdempotentWrite";
            case ALL:
                return "All";
            default:
                return null;
        }
    }
}
