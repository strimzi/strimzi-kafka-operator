/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UpgradeDowngradeDatalist {
    private static final Logger LOGGER = LogManager.getLogger(UpgradeDowngradeDatalist.class);

    private List<UpgradeDowngradeData> data;

    public UpgradeDowngradeDatalist(String fileName) {
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            CollectionType modificationDataListType = mapper.getTypeFactory().constructCollectionType(List.class, UpgradeDowngradeData.class);
            List<UpgradeDowngradeData> datalist = mapper.readValue(new File(TestUtils.USER_PATH + "/src/test/resources/upgrade/" + fileName), modificationDataListType);

            if (fileName.contains("Upgrade")) {
                datalist.forEach(upgradeData -> {
                    // Set upgrade destination data to latest version which is HEAD
                    upgradeData.setUrlTo("HEAD");
                    upgradeData.setToVersion("HEAD");
                    upgradeData.setToExamples("HEAD");
                });
            }

            this.data = datalist;

        } catch (Exception ex) {
            LOGGER.error("Error while parsing ST data from YAML ", ex);
        }
    }

    public List<UpgradeDowngradeData> getData() {
        return data;
    }

    public UpgradeDowngradeData getData(int index) {
        return data.get(index);
    }

    public int getDataSize() {
        return data.size();
    }

    public void setData(List<UpgradeDowngradeData> data) {
        this.data = data;
    }

    public UpgradeDowngradeData buildDataForUpgradeAcrossVersions() {
        List<TestKafkaVersion> sortedVersions = TestKafkaVersion.getSupportedKafkaVersions();
        TestKafkaVersion latestKafkaSupported = sortedVersions.get(sortedVersions.size() - 1);

        UpgradeDowngradeData acrossUpgradeData = getData(getDataSize() - 1);
        UpgradeDowngradeData startingVersion = acrossUpgradeData;

        startingVersion.setDefaultKafka(acrossUpgradeData.getDefaultKafkaVersionPerStrimzi());

        acrossUpgradeData.setFromVersion(startingVersion.getFromVersion());
        acrossUpgradeData.setFromExamples(startingVersion.getFromExamples());
        acrossUpgradeData.setUrlFrom(startingVersion.getUrlFrom());
        acrossUpgradeData.setStartingKafkaVersion(startingVersion.getOldestKafka());
        acrossUpgradeData.setDefaultKafka(startingVersion.getDefaultKafka());
        acrossUpgradeData.setOldestKafka(startingVersion.getOldestKafka());

        // Generate procedures for upgrade
        Map<String, String> procedures = new HashMap<>() {{
                put("kafkaVersion", latestKafkaSupported.version());
                put("logMessageVersion", latestKafkaSupported.messageVersion());
                put("interBrokerProtocolVersion", latestKafkaSupported.protocolVersion());
            }};

        acrossUpgradeData.setProcedures(procedures);

        LOGGER.info("Upgrade yaml for the test: {}", acrossUpgradeData.toString());

        return acrossUpgradeData;
    }
}
