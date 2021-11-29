/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.KafkaVersionTestConstants;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaVersionTestUtils {

    private static final Set<String> SUPPORTED_VERSIONS = new KafkaVersion.Lookup(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()).supportedVersions();

    private static Map<String, String> getKafkaImageMap() {
        return getImageMap(KafkaVersionTestConstants.KAFKA_IMAGE_STR);
    }

    private static Map<String, String> getKafkaConnectImageMap() {
        return getImageMap(KafkaVersionTestConstants.KAFKA_CONNECT_IMAGE_STR);
    }

    private static Map<String, String> getKafkaMirrorMakerImageMap() {
        return getImageMap(KafkaVersionTestConstants.KAFKA_MIRROR_MAKER_IMAGE_STR);
    }

    private static Map<String, String> getKafkaMirrorMaker2ImageMap() {
        return getImageMap(KafkaVersionTestConstants.KAFKA_MIRROR_MAKER_2_IMAGE_STR);
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
                getKafkaMirrorMakerImageMap(),
                getKafkaMirrorMaker2ImageMap());
    }

    public static KafkaVersion getLatestVersion() {
        return getKafkaVersionLookup().version(KafkaVersionTestConstants.LATEST_KAFKA_VERSION);
    }
}
