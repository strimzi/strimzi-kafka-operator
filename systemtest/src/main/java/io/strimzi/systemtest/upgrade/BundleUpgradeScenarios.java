/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import java.util.List;

/**
 * Representation of the BundleUpgrade.yaml file. Scenarios where the Kafka version stays untouched during the whole
 * upgrade are kept separately from the one where the deployed Kafka version is not supported by the Strimzi version
 * we upgrade to, as the latter needs different data and different steps.
 */
public class BundleUpgradeScenarios {
    private List<BundleVersionModificationData> upgrades;
    private BundleVersionModificationData unsupportedKafkaUpgrade;

    public List<BundleVersionModificationData> getUpgrades() {
        return upgrades;
    }

    public void setUpgrades(List<BundleVersionModificationData> upgrades) {
        this.upgrades = upgrades;
    }

    public BundleVersionModificationData getUnsupportedKafkaUpgrade() {
        return unsupportedKafkaUpgrade;
    }

    public void setUnsupportedKafkaUpgrade(BundleVersionModificationData unsupportedKafkaUpgrade) {
        this.unsupportedKafkaUpgrade = unsupportedKafkaUpgrade;
    }
}
