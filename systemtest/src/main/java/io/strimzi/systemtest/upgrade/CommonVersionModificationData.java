/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import java.util.Map;

/**
 * This class contains all the common fields needed for upgrade and downgrade tests from which specific classes
 * inherit these values and extending them further for specific needs of the process.
 */
public class CommonVersionModificationData {
    private String fromVersion;
    private String fromExamples;
    private String fromUrl;
    private String fromKafkaVersionsUrl;
    private String toVersion;
    private String toExamples;
    private String toUrl;
    private UpgradeKafkaVersion procedures;
    private Map<String, String> filePaths;

    public String getFromVersion() {
        return fromVersion;
    }

    public void setFromVersion(String fromVersion) {
        this.fromVersion = fromVersion;
    }

    public String getFromExamples() {
        return fromExamples;
    }

    public void setFromExamples(String fromExamples) {
        this.fromExamples = fromExamples;
    }

    public String getFromUrl() {
        return fromUrl;
    }

    public void setFromUrl(String fromUrl) {
        this.fromUrl = fromUrl;
    }

    public String getFromKafkaVersionsUrl() {
        return fromKafkaVersionsUrl;
    }

    public void setFromKafkaVersionsUrl(String fromKafkaVersionsUrl) {
        this.fromKafkaVersionsUrl = fromKafkaVersionsUrl;
    }

    public String getToVersion() {
        return toVersion;
    }

    public void setToVersion(String toVersion) {
        this.toVersion = toVersion;
    }

    public String getToExamples() {
        return toExamples;
    }

    public void setToExamples(String toExamples) {
        this.toExamples = toExamples;
    }

    public String getToUrl() {
        return toUrl;
    }

    public void setToUrl(String toUrl) {
        this.toUrl = toUrl;
    }

    public UpgradeKafkaVersion getProcedures() {
        return procedures;
    }

    public void setProcedures(UpgradeKafkaVersion procedures) {
        this.procedures = procedures;
    }

    public void setFilePaths(Map<String, String> filePaths) {
        this.filePaths = filePaths;
    }

    public Map<String, String> getFilePaths() {
        return filePaths;
    }

    public String getKafkaFilePathBefore() {
        return getFilePaths().get("kafkaBefore");
    }

    public String getKafkaFilePathAfter() {
        return getFilePaths().get("kafkaAfter");
    }

    public String getKafkaKRaftFilePathBefore() {
        return getFilePaths().get("kafkaKRaftBefore");
    }

    public String getKafkaKRaftFilePathAfter() {
        return getFilePaths().get("kafkaKRaftAfter");
    }

    @Override
    public String toString() {
        return "\n" +
            "CommonVersionModificationData{" +
            ", fromVersion='" + fromVersion + '\'' +
            ", fromExamples='" + fromExamples + '\'' +
            ", fromUrl='" + fromUrl + '\'' +
            ", fromKafkaVersionsUrl='" + fromKafkaVersionsUrl + '\'' +
            ", toVersion='" + toVersion + '\'' +
            ", toExamples='" + toExamples + '\'' +
            ", toUrl='" + toUrl + '\'' +
            "\n}";
    }

}
