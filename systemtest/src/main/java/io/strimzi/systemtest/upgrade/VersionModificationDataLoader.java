/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.params.provider.Arguments;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.type.CollectionType;
import tools.jackson.dataformat.yaml.YAMLMapper;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

/**
 * This Class is a basic data loader that takes type of upgrade as a constructor parameter
 * and loads only data for that specific type of upgrade
 */
public class VersionModificationDataLoader {
    public enum ModificationType {
        OLM_UPGRADE,
        BUNDLE_UPGRADE,
        BUNDLE_DOWNGRADE
    }
    private static final Logger LOGGER = LogManager.getLogger(VersionModificationDataLoader.class);
    private OlmVersionModificationData olmUpgradeData;
    private List<BundleVersionModificationData> bundleVersionModificationDataList;
    private BundleVersionModificationData unsupportedKafkaUpgradeData;

    public VersionModificationDataLoader(ModificationType upgradeType) {
        if (upgradeType == ModificationType.OLM_UPGRADE) {
            loadOlmUpgradeData();
        } else if (upgradeType == ModificationType.BUNDLE_UPGRADE) {
            loadBundleUpgradeData();
        } else if (upgradeType == ModificationType.BUNDLE_DOWNGRADE) {
            loadBundleDowngradeData();
        }
    }

    public void loadBundleUpgradeData() {
        try {
            ObjectMapper mapper = new YAMLMapper();
            BundleUpgradeScenarios scenarios = mapper.readValue(new File(TestUtils.USER_PATH + "/src/test/resources/upgrade/BundleUpgrade.yaml"), BundleUpgradeScenarios.class);

            Stream.concat(scenarios.getUpgrades().stream(), Stream.of(scenarios.getUnsupportedKafkaUpgrade()))
                .forEach(upgradeData -> {
                    // Set upgrade data destination to latest version which is HEAD
                    upgradeData.setToUrl("HEAD");
                    upgradeData.setToVersion("HEAD");
                    upgradeData.setToExamples("HEAD");
                });

            this.bundleVersionModificationDataList = scenarios.getUpgrades();
            this.unsupportedKafkaUpgradeData = scenarios.getUnsupportedKafkaUpgrade();
        } catch (JacksonException e) {
            LOGGER.error("Error while parsing ST data from YAML");
            throw new RuntimeException("Error while parsing ST data from YAML", e);
        }
    }

    public void loadBundleDowngradeData() {
        try {
            ObjectMapper mapper = new YAMLMapper();
            CollectionType modificationDataListType = mapper.getTypeFactory().constructCollectionType(List.class, BundleVersionModificationData.class);
            this.bundleVersionModificationDataList = mapper.readValue(new File(TestUtils.USER_PATH + "/src/test/resources/upgrade/BundleDowngrade.yaml"), modificationDataListType);
        } catch (JacksonException e) {
            LOGGER.error("Error while parsing ST data from YAML");
            throw new RuntimeException("Error while parsing ST data from YAML", e);
        }
    }

    public void loadOlmUpgradeData() {
        try {
            ObjectMapper mapper = new YAMLMapper();
            this.olmUpgradeData = mapper.readValue(new File(TestUtils.USER_PATH + "/src/test/resources/upgrade/OlmUpgrade.yaml"), OlmVersionModificationData.class);
            this.olmUpgradeData.setToUrl("HEAD");
        } catch (JacksonException e) {
            LOGGER.error("Error while parsing ST data from YAML");
            throw new RuntimeException("Error while parsing ST data from YAML", e);
        }
    }

    public OlmVersionModificationData getOlmUpgradeData() {
        return olmUpgradeData;
    }

    public List<BundleVersionModificationData> getBundleUpgradeOrDowngradeDataList() {
        return bundleVersionModificationDataList;
    }

    public BundleVersionModificationData getBundleUpgradeOrDowngradeData(final int index) {
        return bundleVersionModificationDataList.get(index);
    }

    public BundleVersionModificationData buildDataForUpgradeAcrossVersionsForKRaft() {
        BundleVersionModificationData acrossUpgradeData = buildDataForUpgradeAcrossVersions();

        acrossUpgradeData = updateUpgradeDataWithFeatureGates(acrossUpgradeData, null);

        return acrossUpgradeData;
    }

    public BundleVersionModificationData buildDataForUpgradeAcrossVersions() {
        BundleVersionModificationData acrossUpgradeData = getBundleUpgradeOrDowngradeData(0);

        acrossUpgradeData.setProcedures(new UpgradeKafkaVersion(latestSupportedKafkaVersion()));

        LOGGER.info("Upgrade data for the test: {}", acrossUpgradeData);

        return acrossUpgradeData;
    }

    /**
     * Returns data for the scenario upgrading from a Strimzi version running a Kafka version that the Strimzi version
     * we upgrade to no longer supports. The procedures hold the Kafka version the cluster has to be moved to in order
     * to become ready again.
     *
     * @return data for the unsupported Kafka version upgrade scenario
     */
    public BundleVersionModificationData buildDataForUpgradeWithUnsupportedKafkaVersion() {
        unsupportedKafkaUpgradeData.setProcedures(new UpgradeKafkaVersion(latestSupportedKafkaVersion()));

        LOGGER.info("Upgrade data for the unsupported Kafka version test: {}", unsupportedKafkaUpgradeData);

        return unsupportedKafkaUpgradeData;
    }

    private static String latestSupportedKafkaVersion() {
        List<TestKafkaVersion> sortedVersions = TestKafkaVersion.getSupportedKafkaVersions();

        return sortedVersions.get(sortedVersions.size() - 1).version();
    }

    public BundleVersionModificationData buildDataForDowngradeUsingFirstScenarioForKRaft() {
        return buildDataForDowngradeUsingFirstScenario(null);
    }

    /**
     * Picks first downgrade scenario from whole list, adds needed feature gates (if there are some) and returns this updated single scenario.
     * This is used in test cases where we don't want to go through the whole list of downgrade scenarios from BundleDowngrade.yaml file.
     *
     * @param featureGates feature gates that should be added to the whole test scenario
     * @return data for particular downgrade scenario
     */
    public BundleVersionModificationData buildDataForDowngradeUsingFirstScenario(String featureGates) {
        BundleVersionModificationData downgradeData = bundleVersionModificationDataList.get(0);

        return updateUpgradeDataWithFeatureGates(downgradeData, featureGates);
    }

    public static Stream<Arguments> loadYamlDowngradeDataForKRaft() {
        return loadYamlDowngradeDataWithFeatureGates(null);
    }

    public static Stream<Arguments> loadYamlDowngradeDataWithFeatureGates(String featureGates) {
        VersionModificationDataLoader dataLoader = new VersionModificationDataLoader(ModificationType.BUNDLE_DOWNGRADE);
        List<Arguments> parameters = new LinkedList<>();

        List<TestKafkaVersion> testKafkaVersions = TestKafkaVersion.getSupportedKafkaVersions();
        TestKafkaVersion testKafkaVersion = testKafkaVersions.get(0);

        // Generate procedures for upgrade
        UpgradeKafkaVersion procedures = new UpgradeKafkaVersion(testKafkaVersion.version());

        dataLoader.getBundleUpgradeOrDowngradeDataList().forEach(downgradeData -> {
            downgradeData.setProcedures(procedures);

            downgradeData = updateUpgradeDataWithFeatureGates(downgradeData, featureGates);

            parameters.add(Arguments.of(downgradeData.getFromVersion(), downgradeData.getToVersion(), downgradeData.getFromFeatureGates(), downgradeData.getToFeatureGates(), downgradeData));
        });

        return parameters.stream();
    }

    public static Stream<Arguments> loadYamlUpgradeDataForKRaft() {
        return loadYamlUpgradeDataWithFeatureGates(null);
    }

    public static Stream<Arguments> loadYamlUpgradeDataWithFeatureGates(String featureGates) {
        VersionModificationDataLoader upgradeDataList = new VersionModificationDataLoader(ModificationType.BUNDLE_UPGRADE);
        List<Arguments> parameters = new LinkedList<>();

        List<TestKafkaVersion> testKafkaVersions = TestKafkaVersion.getSupportedKafkaVersions();
        TestKafkaVersion testKafkaVersion = testKafkaVersions.get(testKafkaVersions.size() - 1);

        // Generate procedures for upgrade
        UpgradeKafkaVersion procedures = new UpgradeKafkaVersion(testKafkaVersion.version());

        upgradeDataList.getBundleUpgradeOrDowngradeDataList().forEach(upgradeData -> {
            upgradeData.setProcedures(procedures);

            upgradeData = updateUpgradeDataWithFeatureGates(upgradeData, featureGates);

            parameters.add(Arguments.of(
                upgradeData.getFromVersion(), upgradeData.getToVersion(),
                upgradeData.getFromFeatureGates(), upgradeData.getToFeatureGates(),
                upgradeData
            ));
        });

        return parameters.stream();
    }

    private static BundleVersionModificationData updateUpgradeDataWithFeatureGates(BundleVersionModificationData upgradeData, String featureGates) {
        if (featureGates != null && !featureGates.isEmpty()) {
            String fgBefore = upgradeData.getFromFeatureGates();
            String fgAfter = upgradeData.getToFeatureGates();

            // in case that we would like to keep some feature gates, we should replace those from the YAML and use the specified one instead
            // for example in case that we are disabling UTO in YAML, but we need it for KRaft upgrade, we should remove it from the list and
            // keep just specified
            for (String fg : featureGates.split(",")) {
                String fgNameWithoutSign = fg.replace("+", "").replace("-", "");

                fgBefore = fgBefore.replaceFirst("(,?)(\\+|-)" + fgNameWithoutSign, "");
                fgAfter = fgAfter.replaceFirst("(,?)(\\+|-)" + fgNameWithoutSign, "");
            }

            upgradeData.setFromFeatureGates(fgBefore.isEmpty() ?
                featureGates : String.join(",", fgBefore, featureGates));
            upgradeData.setToFeatureGates(fgAfter.isEmpty() ?
                featureGates : String.join(",", fgAfter, featureGates));
        }

        return upgradeData;
    }
}
