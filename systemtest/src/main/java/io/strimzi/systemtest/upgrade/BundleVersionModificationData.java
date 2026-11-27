/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import java.util.Map;

public class BundleVersionModificationData extends CommonVersionModificationData {
    // Bundle specific
    private int additionalTopics;
    private String deployKafkaVersion;
    private Map<String, String> imagesAfterOperations;
    private Map<String, Object> client;

    public Integer getAdditionalTopics() {
        return additionalTopics;
    }

    public String getDeployKafkaVersion() {
        return deployKafkaVersion;
    }

    public Map<String, String> getImagesAfterOperations() {
        return imagesAfterOperations;
    }

    public String getKafkaImage() {
        return imagesAfterOperations.get("kafka");
    }

    /**
     * Builds the expected Kafka image for the given Kafka version, reusing the registry and tag from the configured
     * Kafka image. Scenarios sharing the same data end up on different Kafka versions - some keep the deployed one,
     * others upgrade Kafka as part of the procedure - while the registry and the operator tag stay the same.
     *
     * @param kafkaVersion Kafka version the Pods are expected to run
     *
     * @return expected Kafka image for the given Kafka version
     */
    public String getKafkaImageForVersion(String kafkaVersion) {
        return getKafkaImage().replaceFirst("-kafka-.*$", "-kafka-" + kafkaVersion);
    }

    public String getTopicOperatorImage() {
        return imagesAfterOperations.get("topicOperator");
    }

    public String getUserOperatorImage() {
        return imagesAfterOperations.get("userOperator");
    }

    public Map<String, Object> getClient() {
        return client;
    }

    public int getContinuousClientsMessages() {
        return (int) client.get("continuousClientsMessages");
    }

    public void setAdditionalTopics(Integer additionalTopics) {
        this.additionalTopics = additionalTopics;
    }

    public void setDeployKafkaVersion(String deployKafkaVersion) {
        this.deployKafkaVersion = deployKafkaVersion;
    }

    public void setClient(Map<String, Object> client) {
        this.client = client;
    }

    @Override
    public String toString() {
        return "\n" +
            "BundleVersionModificationData{" +
            super.toString() +
            ", additionalTopics=" + additionalTopics +
            ", deployKafkaVersion='" + deployKafkaVersion + '\'' +
            ", imagesAfterOperations=" + imagesAfterOperations +
            ", client=" + client +
            "\n}";
    }

}
