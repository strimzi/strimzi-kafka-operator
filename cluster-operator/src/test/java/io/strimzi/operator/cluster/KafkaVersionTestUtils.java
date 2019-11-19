/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.strimzi.operator.cluster.model.KafkaVersion;

import java.io.StringReader;
import java.util.Map;

import static io.strimzi.test.TestUtils.map;

public class KafkaVersionTestUtils {

    public static final String KAFKA_VERSION_YAML_TEMPLATE =
                    "- version: %s\n  format: %s\n  protocol: %s\n  zookeeper: %s\n  checksum: %s\n  third-party-libs: %s\n  default: %b\n";
    public static final String KAFKA_IMAGE_STR = "strimzi/kafka:latest-kafka-";
    public static final String KAFKA_CONNECT_IMAGE_STR = "strimzi/kafka-connect:latest-kafka-";
    public static final String KAFKA_CONNECT_S2I_IMAGE_STR = "strimzi/kafka-connect-s2i:latest-kafka-";
    public static final String KAFKA_MIRROR_MAKER_IMAGE_STR = "strimzi/kafka-mirror-maker:latest-kafka-";
    public static final String KAFKA_MIRROR_MAKER_2_IMAGE_STR = "strimzi/kafka-connect:latest-kafka-";

    public static final String LATEST_KAFKA_VERSION = "2.3.0";
    public static final String LATEST_FORMAT_VERSION = "2.3";
    public static final String LATEST_PROTOCOL_VERSION = "2.3";
    public static final String LATEST_ZOOKEEPER_VERSION = "3.4.14";
    public static final String LATEST_CHECKSUM = "ABCD1234";
    public static final String LATEST_THIRD_PARTY_VERSION = "2.3.x";
    public static final String LATEST_KAFKA_IMAGE = KAFKA_IMAGE_STR + LATEST_KAFKA_VERSION;
    public static final String LATEST_KAFKA_CONNECT_IMAGE = KAFKA_CONNECT_IMAGE_STR + LATEST_KAFKA_VERSION;
    public static final String LATEST_KAFKA_CONNECT_S2I_IMAGE = KAFKA_CONNECT_S2I_IMAGE_STR + LATEST_KAFKA_VERSION;
    public static final String LATEST_KAFKA_MIRROR_MAKER_IMAGE = KAFKA_MIRROR_MAKER_IMAGE_STR + LATEST_KAFKA_VERSION;
    public static final String LATEST_KAFKA_MIRROR_MAKER_2_IMAGE = KAFKA_MIRROR_MAKER_2_IMAGE_STR + LATEST_KAFKA_VERSION;


    public static final String PREVIOUS_KAFKA_VERSION = "2.2.1";
    public static final String PREVIOUS_FORMAT_VERSION = "2.2";
    public static final String PREVIOUS_PROTOCOL_VERSION = "2.2";
    public static final String PREVIOUS_ZOOKEEPER_VERSION = "3.4.13";
    public static final String PREVIOUS_CHECKSUM = "ABCD1234";
    public static final String PREVIOUS_THIRD_PARTY_VERSION = "2.2.x";
    public static final String PREVIOUS_KAFKA_IMAGE = KAFKA_IMAGE_STR + PREVIOUS_KAFKA_VERSION;
    public static final String PREVIOUS_KAFKA_CONNECT_IMAGE = KAFKA_CONNECT_IMAGE_STR + PREVIOUS_KAFKA_VERSION;
    public static final String PREVIOUS_KAFKA_CONNECT_S2I_IMAGE = KAFKA_CONNECT_S2I_IMAGE_STR + PREVIOUS_KAFKA_VERSION;
    public static final String PREVIOUS_KAFKA_MIRROR_MAKER_IMAGE = KAFKA_MIRROR_MAKER_IMAGE_STR + PREVIOUS_KAFKA_VERSION;
    public static final String PREVIOUS_KAFKA_MIRROR_MAKER_2_IMAGE = KAFKA_MIRROR_MAKER_2_IMAGE_STR + PREVIOUS_KAFKA_VERSION;

    public static final String PREVIOUS_MINOR_KAFKA_VERSION = "2.2.0";
    public static final String PREVIOUS_MINOR_FORMAT_VERSION = "2.2";
    public static final String PREVIOUS_MINOR_PROTOCOL_VERSION = "2.2";
    public static final String PREVIOUS_MINOR_ZOOKEEPER_VERSION = "3.4.13";
    public static final String PREVIOUS_MINOR_CHECKSUM = "ABCD1234";
    public static final String PREVIOUS_MINOR_THIRD_PARTY_VERSION = "2.2.x";
    public static final String PREVIOUS_MINOR_KAFKA_IMAGE = KAFKA_IMAGE_STR + PREVIOUS_MINOR_KAFKA_VERSION;
    public static final String PREVIOUS_MINOR_KAFKA_CONNECT_IMAGE = KAFKA_CONNECT_IMAGE_STR + PREVIOUS_MINOR_KAFKA_VERSION;
    public static final String PREVIOUS_MINOR_KAFKA_CONNECT_S2I_IMAGE = KAFKA_CONNECT_S2I_IMAGE_STR + PREVIOUS_MINOR_KAFKA_VERSION;
    public static final String PREVIOUS_MINOR_KAFKA_MIRROR_MAKER_IMAGE = KAFKA_MIRROR_MAKER_IMAGE_STR + PREVIOUS_MINOR_KAFKA_VERSION;
    public static final String PREVIOUS_MINOR_KAFKA_MIRROR_MAKER_2_IMAGE = KAFKA_MIRROR_MAKER_2_IMAGE_STR + PREVIOUS_MINOR_KAFKA_VERSION;

    public static final String DEFAULT_KAFKA_VERSION = LATEST_KAFKA_VERSION;
    public static final String DEFAULT_KAFKA_IMAGE = LATEST_KAFKA_IMAGE;
    public static final String DEFAULT_KAFKA_CONNECT_IMAGE = LATEST_KAFKA_CONNECT_IMAGE;
    public static final String DEFAULT_KAFKA_CONNECT_S2I_IMAGE = LATEST_KAFKA_CONNECT_S2I_IMAGE;
    public static final String DEFAULT_KAFKA_MIRROR_MAKER_IMAGE = LATEST_KAFKA_MIRROR_MAKER_IMAGE;

    public static String getPreviousMinorVersionYaml(boolean isDefault) {

        String previousMinorVersionYaml = String.format(
                KAFKA_VERSION_YAML_TEMPLATE,
                PREVIOUS_MINOR_KAFKA_VERSION,
                PREVIOUS_MINOR_FORMAT_VERSION,
                PREVIOUS_MINOR_PROTOCOL_VERSION,
                PREVIOUS_MINOR_ZOOKEEPER_VERSION,
                PREVIOUS_MINOR_CHECKSUM,
                PREVIOUS_MINOR_THIRD_PARTY_VERSION,
                isDefault);

        return previousMinorVersionYaml;
    }

    public static String getPreviousVersionYaml(boolean isDefault) {

        String previousVersionYaml = String.format(
                KAFKA_VERSION_YAML_TEMPLATE,
                PREVIOUS_KAFKA_VERSION,
                PREVIOUS_FORMAT_VERSION,
                PREVIOUS_PROTOCOL_VERSION,
                PREVIOUS_ZOOKEEPER_VERSION,
                PREVIOUS_CHECKSUM,
                PREVIOUS_THIRD_PARTY_VERSION,
                isDefault);

        return previousVersionYaml;
    }

    public static String getLatestVersionYaml(boolean isDefault) {

        String latestVersionYaml = String.format(
                KAFKA_VERSION_YAML_TEMPLATE,
                LATEST_KAFKA_VERSION,
                LATEST_FORMAT_VERSION,
                LATEST_PROTOCOL_VERSION,
                LATEST_ZOOKEEPER_VERSION,
                LATEST_CHECKSUM,
                LATEST_THIRD_PARTY_VERSION,
                isDefault);

        return latestVersionYaml;
    }

    /**
     * Returns a kafka versions yaml string, with three entries, where the latest entry is the default.
     */
    public static String getKafkaVersionYaml() {
        return getPreviousMinorVersionYaml(false) +
                getPreviousVersionYaml(false) +
                getLatestVersionYaml(true);
    }

    public static Map<String, String> getKafkaImageMap() {
        return map(PREVIOUS_MINOR_KAFKA_VERSION, PREVIOUS_MINOR_KAFKA_IMAGE,
                PREVIOUS_KAFKA_VERSION, PREVIOUS_KAFKA_IMAGE,
                LATEST_KAFKA_VERSION, LATEST_KAFKA_IMAGE);
    }

    public static String getKafkaImagesEnvVarString() {
        return PREVIOUS_MINOR_KAFKA_VERSION + "=" + PREVIOUS_MINOR_KAFKA_IMAGE + " " +
                PREVIOUS_KAFKA_VERSION + "=" + PREVIOUS_KAFKA_IMAGE + " " +
                LATEST_KAFKA_VERSION + "=" + LATEST_KAFKA_IMAGE;
    }

    public static Map<String, String> getKafkaConnectImageMap() {
        return map(PREVIOUS_MINOR_KAFKA_VERSION, PREVIOUS_MINOR_KAFKA_CONNECT_IMAGE,
                PREVIOUS_KAFKA_VERSION, PREVIOUS_KAFKA_CONNECT_IMAGE,
                LATEST_KAFKA_VERSION, LATEST_KAFKA_CONNECT_IMAGE);
    }

    public static String getKafkaConnectImagesEnvVarString() {
        return PREVIOUS_MINOR_KAFKA_VERSION + "=" + PREVIOUS_MINOR_KAFKA_CONNECT_IMAGE + " " +
                PREVIOUS_KAFKA_VERSION + "=" + PREVIOUS_KAFKA_CONNECT_IMAGE + " " +
                LATEST_KAFKA_VERSION + "=" + LATEST_KAFKA_CONNECT_IMAGE;
    }

    public static Map<String, String> getKafkaConnectS2iImageMap() {
        return map(PREVIOUS_MINOR_KAFKA_VERSION, PREVIOUS_MINOR_KAFKA_CONNECT_S2I_IMAGE,
                PREVIOUS_KAFKA_VERSION, PREVIOUS_KAFKA_CONNECT_S2I_IMAGE,
                LATEST_KAFKA_VERSION, LATEST_KAFKA_CONNECT_S2I_IMAGE);
    }

    public static String getKafkaConnectS2iImagesEnvVarString() {
        return PREVIOUS_MINOR_KAFKA_VERSION + "=" + PREVIOUS_MINOR_KAFKA_CONNECT_S2I_IMAGE + " " +
                PREVIOUS_KAFKA_VERSION + "=" + PREVIOUS_KAFKA_CONNECT_S2I_IMAGE + " " +
                LATEST_KAFKA_VERSION + "=" + LATEST_KAFKA_CONNECT_S2I_IMAGE;
    }

    public static Map<String, String> getKafkaMirrorMakerImageMap() {
        return map(PREVIOUS_MINOR_KAFKA_VERSION, PREVIOUS_MINOR_KAFKA_MIRROR_MAKER_IMAGE,
                PREVIOUS_KAFKA_VERSION, PREVIOUS_KAFKA_MIRROR_MAKER_IMAGE,
                LATEST_KAFKA_VERSION, LATEST_KAFKA_MIRROR_MAKER_IMAGE);
    }

    public static String getKafkaMirrorMakerImagesEnvVarString() {
        return PREVIOUS_MINOR_KAFKA_VERSION + "=" + PREVIOUS_MINOR_KAFKA_MIRROR_MAKER_IMAGE + " " +
                PREVIOUS_KAFKA_VERSION + "=" + PREVIOUS_KAFKA_MIRROR_MAKER_IMAGE + " " +
                LATEST_KAFKA_VERSION + "=" + LATEST_KAFKA_MIRROR_MAKER_IMAGE;
    }

    public static Map<String, String> getKafkaMirrorMaker2ImageMap() {
        return map(PREVIOUS_MINOR_KAFKA_VERSION, PREVIOUS_MINOR_KAFKA_MIRROR_MAKER_2_IMAGE,
                PREVIOUS_KAFKA_VERSION, PREVIOUS_KAFKA_MIRROR_MAKER_2_IMAGE,
                LATEST_KAFKA_VERSION, LATEST_KAFKA_MIRROR_MAKER_2_IMAGE);
    }

    public static String getKafkaMirrorMaker2ImagesEnvVarString() {
        return PREVIOUS_MINOR_KAFKA_VERSION + "=" + PREVIOUS_MINOR_KAFKA_MIRROR_MAKER_2_IMAGE + " " +
                PREVIOUS_KAFKA_VERSION + "=" + PREVIOUS_KAFKA_MIRROR_MAKER_2_IMAGE + " " +
                LATEST_KAFKA_VERSION + "=" + LATEST_KAFKA_MIRROR_MAKER_2_IMAGE;
    }

    public static KafkaVersion.Lookup getKafkaVersionLookup() {
        return new KafkaVersion.Lookup(
                new StringReader(KafkaVersionTestUtils.getKafkaVersionYaml()),
                KafkaVersionTestUtils.getKafkaImageMap(),
                KafkaVersionTestUtils.getKafkaConnectImageMap(),
                KafkaVersionTestUtils.getKafkaConnectS2iImageMap(),
                KafkaVersionTestUtils.getKafkaMirrorMakerImageMap(),
                KafkaVersionTestUtils.getKafkaMirrorMaker2ImageMap()) {
        };
    }
}
