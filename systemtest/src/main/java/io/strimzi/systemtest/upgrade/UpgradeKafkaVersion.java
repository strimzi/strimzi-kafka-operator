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
    private String logMessageVersion;
    private String interBrokerVersion;

    UpgradeKafkaVersion(TestKafkaVersion testKafkaVersion) {
        this(testKafkaVersion.version(), testKafkaVersion.messageVersion(), testKafkaVersion.protocolVersion());
    }

    UpgradeKafkaVersion(String version) {
        String shortVersion = version;

        if (version != null && !version.equals("")) {
            String[] versionSplit = version.split("\\.");
            shortVersion = String.format("%s.%s", versionSplit[0], versionSplit[1]);
        }

        this.version = version;
        this.logMessageVersion = shortVersion;
        this.interBrokerVersion = shortVersion;
    }

    /**
     * Leaving empty, so original Kafka version in `kafka-persistent.yaml` will be used
     * LMFV and IBPV should be null, so the test steps will for updating the config will be skipped
     */
    UpgradeKafkaVersion() {
        this("", null, null);
    }

    UpgradeKafkaVersion(String version, String logMessageVersion, String interBrokerVersion) {
        this.version = version;
        this.logMessageVersion = logMessageVersion;
        this.interBrokerVersion = interBrokerVersion;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public String getLogMessageVersion() {
        return this.logMessageVersion;
    }

    public String getInterBrokerVersion() {
        return this.interBrokerVersion;
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
