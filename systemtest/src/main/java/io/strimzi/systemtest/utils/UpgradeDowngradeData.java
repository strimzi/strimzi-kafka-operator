/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class UpgradeDowngradeData {
    private static final Logger LOGGER = LogManager.getLogger(UpgradeDowngradeData.class);
    private int additionalTopics;
    private String fromVersion;
    private String fromExamples;
    private String urlFrom;
    private String oldestKafka;
    private String defaultKafka;
    private String deployKafkaVersion;
    private String startingKafkaVersion;
    private String featureGatesBefore;
    private String featureGatesAfter;
    private Map<String, String> conversionTool;
    private Map<String, String> imagesAfterOperations;
    private Map<String, Object> client;
    private Map<String, String> environmentInfo;
    private Map<String, String> procedures;

    // Downgrade specific variables
    private String toVersion;
    private String toExamples;
    private String urlTo;

    public Integer getAdditionalTopics() {
        return additionalTopics;
    }

    public String getFromVersion() {
        return fromVersion;
    }

    public String getFromExamples() {
        return fromExamples;
    }

    public String getUrlFrom() {
        return urlFrom;
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

    public Map<String, String> getConversionTool() {
        return conversionTool;
    }

    public String getToConversionTool() {
        return conversionTool.get("toConversionTool");
    }

    public String getUrlToConversionTool() {
        return conversionTool.get("urlToConversionTool");
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

    public Map<String, String> getProcedures() {
        return procedures;
    }

    public String getProcedureKafkaVersion() {
        return procedures.get("kafkaVersion");
    }

    public String getProcedureLogMessageVersion() {
        return procedures.get("logMessageVersion");
    }

    public String getProcedureInterBrokerProtocolVersion() {
        return procedures.get("interBrokerProtocolVersion");
    }

    public String getToVersion() {
        return toVersion;
    }

    public String getToExamples() {
        return toExamples;
    }

    public String getUrlTo() {
        return urlTo;
    }

    public void setAdditionalTopics(Integer additionalTopics) {
        this.additionalTopics = additionalTopics;
    }

    public void setFromVersion(String fromVersion) {
        this.fromVersion = fromVersion;
    }

    public void setFromExamples(String fromExamples) {
        this.fromExamples = fromExamples;
    }

    public void setUrlFrom(String urlFrom) {
        this.urlFrom = urlFrom;
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

    public void setConversionTool(Map<String, String> conversionTool) {
        this.conversionTool = conversionTool;
    }


    public void setClient(Map<String, Object> client) {
        this.client = client;
    }

    public void setEnvironmentInfo(Map<String, String> environmentInfo) {
        this.environmentInfo = environmentInfo;
    }

    public void setProcedures(Map<String, String> procedures) {
        this.procedures = procedures;
    }

    public void setToVersion(String toVersion) {
        this.toVersion = toVersion;
    }

    public void setToExamples(String toExamples) {
        this.toExamples = toExamples;
    }

    public void setUrlTo(String urlTo) {
        this.urlTo = urlTo;
    }

    public String getDefaultKafkaVersionPerStrimzi() {
        try {
            List<TestKafkaVersion> testKafkaVersions = TestKafkaVersion.parseKafkaVersionsFromUrl("https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/" + getFromVersion() + "/kafka-versions.yaml");
            return testKafkaVersions.stream().filter(TestKafkaVersion::isDefault).collect(Collectors.toList()).get(0).version();
        } catch (Exception e) {
            LOGGER.error("Cannot parse Kafka versions from URL", e);
        }
        throw new RuntimeException("Failed to get Kafka version");
    }
    @Override
    public String toString() {
        return "\n" +
                "VersionModificationData{" +
                "additionalTopics=" + additionalTopics +
                ", fromVersion='" + fromVersion + '\'' +
                ", fromExamples='" + fromExamples + '\'' +
                ", urlFrom='" + urlFrom + '\'' +
                ", oldestKafka='" + oldestKafka + '\'' +
                ", defaultKafka='" + defaultKafka + '\'' +
                ", deployKafkaVersion='" + deployKafkaVersion + '\'' +
                ", startingKafkaVersion='" + startingKafkaVersion + '\'' +
                ", featureGatesBefore='" + featureGatesBefore + '\'' +
                ", featureGatesAfter='" + featureGatesAfter + '\'' +
                ", conversionTool=" + conversionTool +
                ", imagesAfterOperations=" + imagesAfterOperations +
                ", client=" + client +
                ", environmentInfo=" + environmentInfo +
                ", toVersion='" + toVersion + '\'' +
                ", toExamples='" + toExamples + '\'' +
                ", urlTo='" + urlTo + '\'' +
                "\n}";
    }
}
