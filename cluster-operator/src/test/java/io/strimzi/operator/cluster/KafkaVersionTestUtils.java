/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.strimzi.operator.cluster.model.KafkaVersion;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaVersionTestUtils {
    private static final String KAFKA_IMAGE_STR = "strimzi/kafka:latest-kafka-";
    private static final String KAFKA_CONNECT_IMAGE_STR = "strimzi/kafka-connect:latest-kafka-";
    private static final String KAFKA_CONNECT_S2I_IMAGE_STR = "strimzi/kafka-connect-s2i:latest-kafka-";
    private static final String KAFKA_MIRROR_MAKER_IMAGE_STR = "strimzi/kafka-mirror-maker:latest-kafka-";
    private static final String KAFKA_MIRROR_MAKER_2_IMAGE_STR = "strimzi/kafka-connect:latest-kafka-";

    private static final Set<String> SUPPORTED_VERSIONS = new KafkaVersion.Lookup(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()).supportedVersions();

    public static final String LATEST_KAFKA_VERSION = "2.7.0";
    public static final String LATEST_FORMAT_VERSION = "2.7";
    public static final String LATEST_PROTOCOL_VERSION = "2.7";
    public static final String LATEST_ZOOKEEPER_VERSION = "3.5.8";
    public static final String LATEST_CHECKSUM = "ABCD1234";
    public static final String LATEST_THIRD_PARTY_VERSION = "2.7.x";
    public static final String LATEST_KAFKA_IMAGE = KAFKA_IMAGE_STR + LATEST_KAFKA_VERSION;
    public static final String LATEST_KAFKA_CONNECT_IMAGE = KAFKA_CONNECT_IMAGE_STR + LATEST_KAFKA_VERSION;
    public static final String LATEST_KAFKA_CONNECT_S2I_IMAGE = KAFKA_CONNECT_S2I_IMAGE_STR + LATEST_KAFKA_VERSION;
    public static final String LATEST_KAFKA_MIRROR_MAKER_IMAGE = KAFKA_MIRROR_MAKER_IMAGE_STR + LATEST_KAFKA_VERSION;
    public static final String LATEST_KAFKA_MIRROR_MAKER_2_IMAGE = KAFKA_MIRROR_MAKER_2_IMAGE_STR + LATEST_KAFKA_VERSION;

    public static final String PREVIOUS_KAFKA_VERSION = "2.6.1";
    public static final String PREVIOUS_FORMAT_VERSION = "2.6";
    public static final String PREVIOUS_PROTOCOL_VERSION = "2.6";
    public static final String PREVIOUS_ZOOKEEPER_VERSION = "3.5.8";
    public static final String PREVIOUS_CHECKSUM = "ABCD1234";
    public static final String PREVIOUS_THIRD_PARTY_VERSION = "2.6.x";
    public static final String PREVIOUS_KAFKA_IMAGE = KAFKA_IMAGE_STR + PREVIOUS_KAFKA_VERSION;
    public static final String PREVIOUS_KAFKA_CONNECT_IMAGE = KAFKA_CONNECT_IMAGE_STR + PREVIOUS_KAFKA_VERSION;
    public static final String PREVIOUS_KAFKA_CONNECT_S2I_IMAGE = KAFKA_CONNECT_S2I_IMAGE_STR + PREVIOUS_KAFKA_VERSION;
    public static final String PREVIOUS_KAFKA_MIRROR_MAKER_IMAGE = KAFKA_MIRROR_MAKER_IMAGE_STR + PREVIOUS_KAFKA_VERSION;
    public static final String PREVIOUS_KAFKA_MIRROR_MAKER_2_IMAGE = KAFKA_MIRROR_MAKER_2_IMAGE_STR + PREVIOUS_KAFKA_VERSION;

    public static final String DEFAULT_KAFKA_VERSION = LATEST_KAFKA_VERSION;
    public static final String DEFAULT_KAFKA_IMAGE = LATEST_KAFKA_IMAGE;
    public static final String DEFAULT_KAFKA_CONNECT_IMAGE = LATEST_KAFKA_CONNECT_IMAGE;
    public static final String DEFAULT_KAFKA_CONNECT_S2I_IMAGE = LATEST_KAFKA_CONNECT_S2I_IMAGE;
    public static final String DEFAULT_KAFKA_MIRROR_MAKER_IMAGE = LATEST_KAFKA_MIRROR_MAKER_IMAGE;

    private static Map<String, String> getKafkaImageMap() {
        return getImageMap(KAFKA_IMAGE_STR);
    }

    private static Map<String, String> getKafkaConnectImageMap() {
        return getImageMap(KAFKA_CONNECT_IMAGE_STR);
    }

    private static Map<String, String> getKafkaConnectS2iImageMap() {
        return getImageMap(KAFKA_CONNECT_S2I_IMAGE_STR);
    }

    private static Map<String, String> getKafkaMirrorMakerImageMap() {
        return getImageMap(KAFKA_MIRROR_MAKER_IMAGE_STR);
    }

    private static Map<String, String> getKafkaMirrorMaker2ImageMap() {
        return getImageMap(KAFKA_MIRROR_MAKER_2_IMAGE_STR);
    }

    private static String envVarFromMap(Map<String, String> imageMap)   {
        return imageMap.entrySet().stream().map(image -> image.getKey() + "=" + image.getValue()).collect(Collectors.joining(" "));
    }

    public static String getKafkaImagesEnvVarString() {
        return envVarFromMap(getKafkaImageMap());
    }

    public static String getKafkaConnectImagesEnvVarString() {
        return envVarFromMap(getKafkaConnectImageMap());
    }

    public static String getKafkaConnectS2iImagesEnvVarString() {
        return envVarFromMap(getKafkaConnectS2iImageMap());
    }

    public static String getKafkaMirrorMakerImagesEnvVarString() {
        return envVarFromMap(getKafkaMirrorMakerImageMap());
    }

    public static String getKafkaMirrorMaker2ImagesEnvVarString() {
        return envVarFromMap(getKafkaMirrorMaker2ImageMap());
    }

    private static Map<String, String> getImageMap(String imageNameBase)    {
        Map<String, String> imageMap = new HashMap<>(SUPPORTED_VERSIONS.size());

        for (String version : SUPPORTED_VERSIONS)    {
            imageMap.put(version, imageNameBase + version);
        }

        return imageMap;
    }

    public static KafkaVersion.Lookup getKafkaVersionLookup() {
        return new KafkaVersion.Lookup(
                getKafkaImageMap(),
                getKafkaConnectImageMap(),
                getKafkaConnectS2iImageMap(),
                getKafkaMirrorMakerImageMap(),
                getKafkaMirrorMaker2ImageMap());
    }

    public static KafkaVersion getLatestVersion() {
        return getKafkaVersionLookup().version(KafkaVersionTestUtils.LATEST_KAFKA_VERSION);
    }
}
