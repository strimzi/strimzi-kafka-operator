/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.List;

public class UpgradeDowngradeDatalist {
    private static final Logger LOGGER = LogManager.getLogger(UpgradeDowngradeDatalist.class);

    private List<UpgradeDowngradeData> upgradeData;
    private List<UpgradeDowngradeData> downgradeData;

    public UpgradeDowngradeDatalist() {
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            CollectionType modificationDataListType = mapper.getTypeFactory().constructCollectionType(List.class, UpgradeDowngradeData.class);
            List<UpgradeDowngradeData> upgradeDatalist = mapper.readValue(new File(TestUtils.USER_PATH + "/src/test/resources/upgrade/StrimziUpgradeST.yaml"), modificationDataListType);

            upgradeDatalist.forEach(upgradeData -> {
                // Set upgrade data destination to latest version which is HEAD
                upgradeData.setUrlTo("HEAD");
                upgradeData.setToVersion("HEAD");
                upgradeData.setToExamples("HEAD");
            });
            this.upgradeData = upgradeDatalist;
            this.downgradeData = mapper.readValue(new File(TestUtils.USER_PATH + "/src/test/resources/upgrade/StrimziDowngradeST.yaml"), modificationDataListType);

        } catch (Exception e) {
            LOGGER.error("Error while parsing ST data from YAML ");
            throw new RuntimeException(e);
        }
    }

    public List<UpgradeDowngradeData> getUpgradeData() {
        return upgradeData;
    }

    public UpgradeDowngradeData getUpgradeData(int index) {
        return upgradeData.get(index);
    }

    public int getUpgradeDataSize() {
        return upgradeData.size();
    }

    public List<UpgradeDowngradeData> getDowngradeData() {
        return downgradeData;
    }

    public UpgradeDowngradeData buildDataForUpgradeAcrossVersions() {
        List<TestKafkaVersion> sortedVersions = TestKafkaVersion.getSupportedKafkaVersions();
        TestKafkaVersion latestKafkaSupported = sortedVersions.get(sortedVersions.size() - 1);

        UpgradeDowngradeData acrossUpgradeData = getUpgradeData(getUpgradeDataSize() - 1);
        UpgradeDowngradeData startingVersion = acrossUpgradeData;

        startingVersion.setDefaultKafka(acrossUpgradeData.getDefaultKafkaVersionPerStrimzi());

        acrossUpgradeData.setFromVersion(startingVersion.getFromVersion());
        acrossUpgradeData.setFromExamples(startingVersion.getFromExamples());
        acrossUpgradeData.setUrlFrom(startingVersion.getUrlFrom());
        acrossUpgradeData.setStartingKafkaVersion(startingVersion.getOldestKafka());
        acrossUpgradeData.setDefaultKafka(startingVersion.getDefaultKafka());
        acrossUpgradeData.setOldestKafka(startingVersion.getOldestKafka());

        // Generate procedures for upgrade
        UpgradeKafkaVersion procedures = new UpgradeKafkaVersion(latestKafkaSupported.version());

        acrossUpgradeData.setProcedures(procedures);

        LOGGER.info("Upgrade yaml for the test: {}", acrossUpgradeData.toString());

        return acrossUpgradeData;
    }
}
