/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import io.strimzi.systemtest.utils.TestKafkaVersion;

/**
 * Class for representing Kafka version, with LMFV and IBPV for our upgrade/downgrade tests
 * Represents "procedures" which should be done after upgrade of operator/before downgrade of operator
 */
public class UpgradeKafkaVersion {

    private String version;
    private String metadataVersion;

    public UpgradeKafkaVersion(TestKafkaVersion testKafkaVersion) {
        this(testKafkaVersion.version());
    }

    public UpgradeKafkaVersion(String version, String desiredMetadataVersion) {
        this.version = version;
        this.metadataVersion = desiredMetadataVersion;
    }

    public UpgradeKafkaVersion(String version) {
        String shortVersion = version;

        if (version != null && !version.isEmpty()) {
            String[] versionSplit = version.split("\\.");
            shortVersion = String.format("%s.%s", versionSplit[0], versionSplit[1]);
        }

        this.version = version;
        this.metadataVersion = shortVersion;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setMetadataVersion(String metadataVersion) {
        this.metadataVersion = metadataVersion;
    }

    public String getVersion() {
        return version;
    }

    public String getMetadataVersion() {
        return this.metadataVersion;
    }

    public static UpgradeKafkaVersion getKafkaWithVersionFromUrl(String kafkaVersionsUrl, String kafkaVersion) {
        if (kafkaVersionsUrl.equals("HEAD")) {
            return new UpgradeKafkaVersion(TestKafkaVersion.getSpecificVersion(kafkaVersion));
        } else {
            try {
                TestKafkaVersion testKafkaVersion = TestKafkaVersion.getSpecificVersionFromList(
                    TestKafkaVersion.parseKafkaVersionsFromUrl(kafkaVersionsUrl), kafkaVersion
                );
                return new UpgradeKafkaVersion(testKafkaVersion);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage());
            }
        }
    }
}
