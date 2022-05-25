/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.Map;

public class VersionModificationData {
    private static final Logger LOGGER = LogManager.getLogger(VersionModificationData.class);
    private Integer additionalTopics;
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
    private Map<String, String> imagesAfterKafkaUpgrade;
    private Map<String, Object> client;
    private Map<String, String> environmentInfo;

    // Downgrade specific variables
    private String toVersion;
    private String toExamples;
    private String urlTo;
    private Map<String, String> imagesAfterOperatorDowngrade;

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

    public Map<String, String> getImagesAfterKafkaUpgrade() {
        return imagesAfterKafkaUpgrade;
    }

    public Map<String, Object> getClient() {
        return client;
    }

    public Map<String, String> getEnvironmentInfo() {
        return environmentInfo;
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

    public Map<String, String> getImagesAfterOperatorDowngrade() {
        return imagesAfterOperatorDowngrade;
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

    public void setImagesAfterKafkaUpgrade(Map<String, String> imagesAfterKafkaUpgrade) {
        this.imagesAfterKafkaUpgrade = imagesAfterKafkaUpgrade;
    }

    public void setClient(Map<String, Object> client) {
        this.client = client;
    }

    public void setEnvironmentInfo(Map<String, String> environmentInfo) {
        this.environmentInfo = environmentInfo;
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

    public void setImagesAfterOperatorDowngrade(Map<String, String> imagesAfterOperatorDowngrade) {
        this.imagesAfterOperatorDowngrade = imagesAfterOperatorDowngrade;
    }

    @Override
    public String toString() {
        return "VersionModificationData{" +
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
                ", imagesAfterKafkaUpgrade=" + imagesAfterKafkaUpgrade +
                ", client=" + client +
                ", environmentInfo=" + environmentInfo +
                ", toVersion='" + toVersion + '\'' +
                ", toExamples='" + toExamples + '\'' +
                ", urlTo='" + urlTo + '\'' +
                ", imagesAfterOperatorDowngrade=" + imagesAfterOperatorDowngrade +
                '}';
    }
}
