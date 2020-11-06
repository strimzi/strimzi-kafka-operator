/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.params.provider.Arguments;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class AbstractUpgradeST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(AbstractUpgradeST.class);

    protected static JsonArray readUpgradeJson() {
        try (InputStream fis = new FileInputStream(TestUtils.USER_PATH + "/src/main/resources/StrimziUpgradeST.json")) {
            JsonReader reader = Json.createReader(fis);
            return reader.readArray();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(TestUtils.USER_PATH + "/src/main/resources/StrimziUpgradeST.json" + " file was not found.");
        }
    }

    /**
     * List cluster operator versions which supports specific kafka version. It uses StrimziUpgradeST.json file to parse
     * the 'toVersion' and 'proceduresAfter' JsonObjects.
     * example:
     *      2.6.0->[HEAD, 6.6.6]
     *      2.3.1->[0.15.0]
     *      2.4.0->[0.16.2, 0.17.0]
     *      2.5.0->[0.18.0, 0.19.0]
     *      2.2.1->[0.12.1]
     *      2.3.0->[0.13.0, 0.14.0]
     * @return map key -> kafka version | value -> list of cluster operator versions
     */
    protected static Map<String, List<String>> getMapKafkaVersionsWithSupportedClusterOperatorVersions() {
        // message format -> [co versions]
        Map<String, List<String>> mapSupportedLogMessageVersions = new HashMap<>();
        String[] previousKafkaVersion = new String[1];

        Objects.requireNonNull(AbstractUpgradeST.readUpgradeJson()).stream().iterator().forEachRemaining(
            item -> {
                String clusterOperatorVersion = item.asJsonObject().getString("toVersion");
                String kafkaVersion = item.asJsonObject().getJsonObject("proceduresAfter").asJsonObject().getString("kafkaVersion");

                LOGGER.debug("Cluster operator version is: {}", clusterOperatorVersion);
                LOGGER.debug("Kafka version: {}", kafkaVersion);

                // if contains kafka version
                if (mapSupportedLogMessageVersions.containsKey(kafkaVersion)) {
                    mapSupportedLogMessageVersions.get(kafkaVersion).add(clusterOperatorVersion);
                } else if (kafkaVersion.equals("")) {
                    mapSupportedLogMessageVersions.get(previousKafkaVersion[0]).add(clusterOperatorVersion);
                } else {
                    // first list created
                    if (mapSupportedLogMessageVersions.get(kafkaVersion) == null) {
                        List<String> clusterOperatorVersions = new ArrayList<>(4);
                        clusterOperatorVersions.add(clusterOperatorVersion);
                        mapSupportedLogMessageVersions.put(kafkaVersion, clusterOperatorVersions);
                    } else {
                        // already created and just appending
                        // adding another co version to specific kafka version
                        mapSupportedLogMessageVersions.put(kafkaVersion, mapSupportedLogMessageVersions.get(kafkaVersion));
                        mapSupportedLogMessageVersions.get(kafkaVersion).add(clusterOperatorVersion);
                    }
                }

                // skipping if kafka version is empty and does not override previous one...
                if (!kafkaVersion.equals("")) {
                    previousKafkaVersion[0] = kafkaVersion;
                }

                mapSupportedLogMessageVersions.forEach((logVersion, supportedCoVersions) -> {
                    LOGGER.debug(logVersion + "->" + supportedCoVersions.toString());
                });
            }
        );

        List<String> kafkaLatestVersionSupportedByCo = mapSupportedLogMessageVersions.get(Environment.ST_KAFKA_VERSION);
        // adding also 6.6.6 because HEAD and 6.6.6 is the same and must support latest version
        kafkaLatestVersionSupportedByCo.add(Environment.OLM_LATEST_CONTAINER_IMAGE_TAG_DEFAULT);
        mapSupportedLogMessageVersions.put(Environment.ST_KAFKA_VERSION, kafkaLatestVersionSupportedByCo);

        return mapSupportedLogMessageVersions;
    }

    protected static Stream<Arguments> loadJsonUpgradeData() {
        JsonArray upgradeData = readUpgradeJson();
        List<Arguments> parameters = new LinkedList<>();

        upgradeData.forEach(jsonData -> {
            JsonObject data = (JsonObject) jsonData;
            parameters.add(Arguments.of(data.getString("fromVersion"), data.getString("toVersion"), data));
        });

        return parameters.stream();
    }
}
