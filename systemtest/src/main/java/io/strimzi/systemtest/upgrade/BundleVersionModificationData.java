/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import io.strimzi.systemtest.utils.TestKafkaVersion;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BundleVersionModificationData extends CommonVersionModificationData {
    private static final Logger LOGGER = LogManager.getLogger(BundleVersionModificationData.class);
    // Bundle specific
    private int additionalTopics;
    private String oldestKafka;
    private String defaultKafka;
    private String deployKafkaVersion;
    private String startingKafkaVersion;
    private String featureGatesBefore;
    private String featureGatesAfter;
    private Map<String, String> imagesAfterOperations;
    private Map<String, Object> client;
    private Map<String, String> environmentInfo;

    public Integer getAdditionalTopics() {
        return additionalTopics;
    }

    public String getOldestKafka() {
        return oldestKafka;
    }

    public String getDefaultKafka() {
        return defaultKafka;
    }

    public String getDeployKafkaVersion() {
        return deployKafkaVersion;
    }

    public String getStartingKafkaVersion() {
        return startingKafkaVersion;
    }

    public String getFeatureGatesBefore() {
        return featureGatesBefore;
    }

    public String getFeatureGatesAfter() {
        return featureGatesAfter;
    }

    public Map<String, String> getImagesAfterOperations() {
        return imagesAfterOperations;
    }

    public String getZookeeperImage() {
        return imagesAfterOperations.get("zookeeper");
    }

    public String getKafkaImage() {
        return imagesAfterOperations.get("kafka");
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

    public Map<String, String> getEnvironmentInfo() {
        return environmentInfo;
    }

    public String getEnvMaxK8sVersion() {
        return environmentInfo.get("maxK8sVersion");
    }

    public String getEnvStatus() {
        return environmentInfo.get("status");
    }

    public String getEnvFlakyVariable() {
        return environmentInfo.get("flakyEnvVariable");
    }

    public String getEnvReason() {
        return environmentInfo.get("reason");
    }

    public void setAdditionalTopics(Integer additionalTopics) {
        this.additionalTopics = additionalTopics;
    }

    public void setOldestKafka(String oldestKafka) {
        this.oldestKafka = oldestKafka;
    }

    public void setDefaultKafka(String defaultKafka) {
        this.defaultKafka = defaultKafka;
    }

    public void setDeployKafkaVersion(String deployKafkaVersion) {
        this.deployKafkaVersion = deployKafkaVersion;
    }

    public void setStartingKafkaVersion(String startingKafkaVersion) {
        this.startingKafkaVersion = startingKafkaVersion;
    }

    public void setFeatureGatesBefore(String featureGatesBefore) {
        this.featureGatesBefore = featureGatesBefore;
    }

    public void setFeatureGatesAfter(String featureGatesAfter) {
        this.featureGatesAfter = featureGatesAfter;
    }

    public void setClient(Map<String, Object> client) {
        this.client = client;
    }

    public void setEnvironmentInfo(Map<String, String> environmentInfo) {
        this.environmentInfo = environmentInfo;
    }

    public String getDefaultKafkaVersionPerStrimzi() {
        try {
            List<TestKafkaVersion> testKafkaVersions = TestKafkaVersion.parseKafkaVersionsFromUrl(getFromKafkaVersionsUrl());
            return testKafkaVersions.stream().filter(TestKafkaVersion::isDefault).collect(Collectors.toList()).get(0).version();
        } catch (Exception e) {
            LOGGER.error("Cannot parse Kafka versions from URL");
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "\n" +
            "BundleVersionModificationData{" +
            super.toString() +
            ", additionalTopics=" + additionalTopics +
            ", oldestKafka='" + oldestKafka + '\'' +
            ", defaultKafka='" + defaultKafka + '\'' +
            ", deployKafkaVersion='" + deployKafkaVersion + '\'' +
            ", startingKafkaVersion='" + startingKafkaVersion + '\'' +
            ", featureGatesBefore='" + featureGatesBefore + '\'' +
            ", featureGatesAfter='" + featureGatesAfter + '\'' +
            ", imagesAfterOperations=" + imagesAfterOperations +
            ", client=" + client +
            ", environmentInfo=" + environmentInfo +
            "\n}";
    }

}
