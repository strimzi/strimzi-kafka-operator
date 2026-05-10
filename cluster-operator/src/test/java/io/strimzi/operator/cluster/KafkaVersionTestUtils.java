/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.KafkaVersionChange;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaVersionTestUtils {
    private static final String KAFKA_IMAGE_STR = "strimzi/kafka:latest-kafka-";
    private static final Set<String> SUPPORTED_VERSIONS = new KafkaVersion.Lookup(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()).supportedVersions();

    public static final String LATEST_KAFKA_VERSION;
    public static final String LATEST_METADATA_VERSION;
    public static final String LATEST_KAFKA_IMAGE;
    static {
        String v = SUPPORTED_VERSIONS.stream().max(KafkaVersion::compareDottedVersions).orElseThrow(() -> new RuntimeException("Failed to find the latest Kafka version"));
        KafkaVersion version = getKafkaVersionLookup().version(v);
        LATEST_KAFKA_VERSION = version.version();
        LATEST_METADATA_VERSION = version.metadataVersion();
        LATEST_KAFKA_IMAGE = KAFKA_IMAGE_STR + version.version();
    }

    public static final String PREVIOUS_KAFKA_VERSION;
    public static final String PREVIOUS_METADATA_VERSION;
    public static final String PREVIOUS_KAFKA_IMAGE;
    static {
        String v = SUPPORTED_VERSIONS.stream().min(KafkaVersion::compareDottedVersions).orElseThrow(() -> new RuntimeException("Failed to find the latest Kafka version"));
        KafkaVersion version = getKafkaVersionLookup().version(v);
        PREVIOUS_KAFKA_VERSION = version.version();
        PREVIOUS_METADATA_VERSION = version.metadataVersion();
        PREVIOUS_KAFKA_IMAGE = KAFKA_IMAGE_STR + version.version();
    }

    public static final String DEFAULT_KAFKA_VERSION;
    public static final String DEFAULT_METADATA_VERSION;
    public static final String DEFAULT_KAFKA_IMAGE;
    static {
        KafkaVersion version = getKafkaVersionLookup().defaultVersion();
        DEFAULT_KAFKA_VERSION = version.version();
        DEFAULT_METADATA_VERSION = version.metadataVersion();
        DEFAULT_KAFKA_IMAGE = KAFKA_IMAGE_STR + version.version();
    }
    
    public static final String HIGH_UNKNOWN_KAFKA_VERSION = "99.0.0";
    public static final String LOW_UNKNOWN_KAFKA_VERSION = "3.99.0";

    public static final KafkaVersionChange DEFAULT_KRAFT_VERSION_CHANGE = new KafkaVersionChange(getKafkaVersionLookup().defaultVersion(), getKafkaVersionLookup().defaultVersion(), null, null, getKafkaVersionLookup().defaultVersion().metadataVersion());

    private static Map<String, String> getKafkaImageMap() {
        return getImageMap(KAFKA_IMAGE_STR);
    }

    private static Map<String, String> getKafkaConnectImageMap() {
        return getImageMap(KAFKA_IMAGE_STR);
    }

    private static Map<String, String> getKafkaMirrorMaker2ImageMap() {
        return getImageMap(KAFKA_IMAGE_STR);
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
                getKafkaMirrorMaker2ImageMap());
    }

    public static KafkaVersion getLatestVersion() {
        return getKafkaVersionLookup().version(KafkaVersionTestUtils.LATEST_KAFKA_VERSION);
    }
}
