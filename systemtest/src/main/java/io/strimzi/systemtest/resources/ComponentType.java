/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources;

public enum ComponentType {
    Kafka,
    KafkaBridge,
    KafkaConnect,
    KafkaMirrorMaker,
    KafkaMirrorMaker2,
    CruiseControl,
    Zookeeper,
    KafkaExporter,
    UserOperator,
    TopicOperator,
    ClusterOperator,
}
