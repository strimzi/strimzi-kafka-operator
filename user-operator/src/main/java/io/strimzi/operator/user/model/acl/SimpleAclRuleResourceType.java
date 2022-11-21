/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model.acl;


/**
 * Enum representing Kafka resource type.
 * Currently supports only Topic, Groups and Clusters.
 * TransactionIds and Delegation tokens are currently unsupported.
 */
public enum SimpleAclRuleResourceType {
    /**
     * Topic resource
     */
    TOPIC,

    /**
     * Group resource
     */
    GROUP,

    /**
     * Cluster resource
     */
    CLUSTER,

    /**
     * Transactional ID resource
     */
    TRANSACTIONAL_ID;
}
