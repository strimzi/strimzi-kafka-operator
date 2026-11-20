/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaSpec;

/**
 * Holds information about which of the optional operand components connecting to the Kafka cluster are deployed. It
 * is used to configure only the users / principals of the components that actually exist (for example in the
 * super-users list or in the list of Service Accounts allowed to connect to the replication listener).
 *
 * @param topicOperator     Indicates whether the Topic Operator is deployed
 * @param userOperator      Indicates whether the User Operator is deployed
 * @param kafkaExporter     Indicates whether the Kafka Exporter is deployed
 * @param extraMetrics      Indicates whether Extra Metrics is deployed
 * @param cruiseControl     Indicates whether Cruise Control is deployed
 */
public record KafkaClusterDeployedComponents(boolean topicOperator, boolean userOperator, boolean kafkaExporter, boolean extraMetrics, boolean cruiseControl) {
    /**
     * Constant with all optional components deployed
     */
    public static final KafkaClusterDeployedComponents ALL = new KafkaClusterDeployedComponents(true, true, true, true, true);

    /**
     * Constant with none of the optional components deployed
     */
    public static final KafkaClusterDeployedComponents NONE = new KafkaClusterDeployedComponents(false, false, false, false, false);

    /**
     * Creates the KafkaClusterDeployedComponents instance from the Kafka custom resource
     *
     * @param kafka     Kafka custom resource
     *
     * @return  KafkaClusterDeployedComponents instance describing which components are deployed
     */
    @SuppressWarnings("deprecation") // Kafka Exporter is deprecated, but still supported
    public static KafkaClusterDeployedComponents fromCrd(Kafka kafka) {
        KafkaSpec spec = kafka.getSpec();

        return new KafkaClusterDeployedComponents(
                spec.getEntityOperator() != null && spec.getEntityOperator().getTopicOperator() != null,
                spec.getEntityOperator() != null && spec.getEntityOperator().getUserOperator() != null,
                spec.getKafkaExporter() != null,
                spec.getExtraMetrics() != null,
                spec.getCruiseControl() != null
        );
    }

    /**
     * Indicates whether the Entity Operator is deployed. The Topic and User Operators are both part of the Entity
     * Operator and share its Service Account.
     *
     * @return  True if the Entity Operator is deployed (i.e. when at least one of the Topic or User Operators is
     *          deployed). False otherwise.
     */
    public boolean entityOperator() {
        return topicOperator || userOperator;
    }
}
