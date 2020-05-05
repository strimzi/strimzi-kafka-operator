/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaUpgradeException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * Represents a Kafka version that's supported by this CO
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaVersion implements Comparable<KafkaVersion> {

    /**
     * Parse the version information present in the {@code /kafka-versions} classpath resource.
     * @param reader A reader for the Kafka version file.
     * @param mapOfVersions A map to add the versions to.
     * @return The configured default Kafka version.
     * @throws IllegalArgumentException If there are duplicate versions listed in the versions file or more than one
     *                                  version is listed as the default.
     * @throws RuntimeException If no default version was set.
     * @throws IOException If the Kafka versions file cannot be read.
     */
    public static KafkaVersion parseKafkaVersions(Reader reader, Map<String, KafkaVersion> mapOfVersions)
            throws IOException, IllegalArgumentException {

        YAMLMapper mapper = new YAMLMapper();

        List<KafkaVersion> kafkaVersions = mapper.readValue(reader, new TypeReference<List<KafkaVersion>>() { });

        KafkaVersion defaultVersion = null;

        for (KafkaVersion kafkaVersion: kafkaVersions) {

            if (mapOfVersions.put(kafkaVersion.version, kafkaVersion) != null) {
                throw new IllegalArgumentException("Duplicate version (" + kafkaVersion.version + ") listed in kafka-versions file");
            }

            if (kafkaVersion.isDefault) {
                if (defaultVersion == null) {
                    defaultVersion = kafkaVersion;
                } else {
                    throw new IllegalArgumentException(
                            "Multiple Kafka versions (" + defaultVersion.version() + " and " + kafkaVersion.version() +
                                    ") are set as default");
                }
            }
        }

        if (defaultVersion != null) {
            return defaultVersion;
        } else {
            throw new RuntimeException("No Kafka version was configured as the default");
        }
    }

    public static class Lookup {
        public static final String KAFKA_VERSIONS_RESOURCE = "kafka-versions.yaml";
        private final Map<String, KafkaVersion> map;
        private final KafkaVersion defaultVersion;
        private final Map<String, String> kafkaImages;
        private final Map<String, String> kafkaConnectImages;
        private final Map<String, String> kafkaConnectS2iImages;
        private final Map<String, String> kafkaMirrorMakerImages;
        private final Map<String, String> kafkaMirrorMaker2Images;

        public Lookup(Map<String, String> kafkaImages,
                      Map<String, String> kafkaConnectImages,
                      Map<String, String> kafkaConnectS2iImages,
                      Map<String, String> kafkaMirrorMakerImages,
                      Map<String, String> kafkaMirrorMaker2Images) {
            this(new InputStreamReader(
                    KafkaVersion.class.getResourceAsStream("/" + KAFKA_VERSIONS_RESOURCE),
                    StandardCharsets.UTF_8),
                    kafkaImages, kafkaConnectImages, kafkaConnectS2iImages, kafkaMirrorMakerImages, kafkaMirrorMaker2Images);
        }

        protected Lookup(Reader reader,
                         Map<String, String> kafkaImages,
                         Map<String, String> kafkaConnectImages,
                         Map<String, String> kafkaConnectS2iImages,
                         Map<String, String> kafkaMirrorMakerImages,
                         Map<String, String> kafkaMirrorMaker2Images) {
            map = new HashMap<>(5);
            try {
                defaultVersion = parseKafkaVersions(reader, map);
            } catch (Exception e) {
                throw new RuntimeException("Error reading " + KAFKA_VERSIONS_RESOURCE, e);
            }
            this.kafkaImages = kafkaImages;
            this.kafkaConnectImages = kafkaConnectImages;
            this.kafkaConnectS2iImages = kafkaConnectS2iImages;
            this.kafkaMirrorMakerImages = kafkaMirrorMakerImages;
            this.kafkaMirrorMaker2Images = kafkaMirrorMaker2Images;
        }

        public KafkaVersion defaultVersion() {
            return defaultVersion;
        }

        /** Find the version from the given version string.
         * @param version The version.
         * @return The KafkaVersion.
         */
        public KafkaVersion version(String version) {
            KafkaVersion result;
            if (version == null) {
                result = defaultVersion;
            } else {
                result = map.get(version);
            }
            if (result == null) {
                throw new KafkaUpgradeException(String.format(
                        "Unsupported Kafka.spec.kafka.version: %s. " +
                                "Supported versions are: %s",
                        version, map.keySet()));
            }
            return result;
        }

        public Set<String> supportedVersions() {
            return new TreeSet<>(map.keySet());
        }

        public Set<String> supportedVersionsForFeature(String feature) {
            return map.entrySet().stream()
                    .filter(entry -> entry.getValue().unsupportedFeatures() == null || !entry.getValue().unsupportedFeatures().contains(feature))
                    .map(Entry::getKey)
                    .collect(Collectors.toSet());
        }

        private String image(final String crImage, final String crVersion, Map<String, String> images, String envVar)
                throws NoImageException {
            final String image;
            if (crImage == null) {
                if (crVersion == null) {
                    image = images.get(defaultVersion().version());
                    if (image == null) {
                        throw new NoImageException("No image for default version " + defaultVersion() + " in " + images.toString());
                    }
                } else {
                    image = images.get(crVersion);
                    if (image == null) {
                        throw new NoImageException("No image for version " + crVersion + " in " + images.toString());
                    }
                }
            } else {
                image = crImage;
            }
            return image;
        }

        /**
         * The Kafka image to use for a Kafka cluster.
         * @param image The image given in the CR.
         * @param version The version given in the CR.
         * @return The image to use.
         * @throws InvalidResourceException If no image was given in the CR and the version given
         * was not present in {@link ClusterOperatorConfig#STRIMZI_KAFKA_IMAGES}.
         */
        public String kafkaImage(String image, String version) {
            try {
                return image(image, version, kafkaImages, ClusterOperatorConfig.STRIMZI_KAFKA_IMAGES);
            } catch (NoImageException e) {
                throw asInvalidResourceException(version, e);
            }
        }

        /**
         * Validate that the given versions have images present in {@link ClusterOperatorConfig#STRIMZI_KAFKA_IMAGES}.
         * @param versions The versions to validate.
         * @throws NoImageException If one of the versions lacks an image.
         */
        public void validateKafkaImages(Iterable<String> versions) throws NoImageException {
            for (String version : versions) {
                image(null, version, kafkaImages, ClusterOperatorConfig.STRIMZI_KAFKA_IMAGES);
            }
        }

        /**
         * The Kafka Connect image to use for a Kafka Connect cluster.
         * @param image The image given in the CR.
         * @param version The version given in the CR.
         * @return The image to use.
         * @throws InvalidResourceException If no image was given in the CR and the version given
         * was not present in {@link ClusterOperatorConfig#STRIMZI_KAFKA_CONNECT_IMAGES}.
         */
        public String kafkaConnectVersion(String image, String version) {
            try {
                return image(image,
                        version,
                        kafkaConnectImages,
                        ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_IMAGES);
            } catch (NoImageException e) {
                throw asInvalidResourceException(version, e);
            }
        }

        /**
         * Validate that the given versions have images present in {@link ClusterOperatorConfig#STRIMZI_KAFKA_CONNECT_IMAGES}.
         * @param versions The versions to validate.
         * @throws NoImageException If one of the versions lacks an image.
         */
        public void validateKafkaConnectImages(Iterable<String> versions) throws NoImageException {
            for (String version : versions) {
                image(null, version, kafkaConnectImages, ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_IMAGES);
            }
        }

        /**
         * The Kafka Connect S2I image to use for a Kafka Connect cluster.
         * @param image The image given in the CR.
         * @param version The version given in the CR.
         * @return The image to use.
         * @throws InvalidResourceException If no image was given in the CR and the version given
         * was not present in {@link ClusterOperatorConfig#STRIMZI_KAFKA_CONNECT_S2I_IMAGES}.
         */
        public String kafkaConnectS2IVersion(String image, String version) {
            try {
                return image(image,
                        version,
                        kafkaConnectS2iImages,
                        ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_S2I_IMAGES);
            } catch (NoImageException e) {
                throw asInvalidResourceException(version, e);
            }
        }

        /**
         * Validate that the given versions have images present in {@link ClusterOperatorConfig#STRIMZI_KAFKA_CONNECT_S2I_IMAGES}.
         * @param versions The versions to validate.
         * @throws NoImageException If one of the versions lacks an image.
         */
        public void validateKafkaConnectS2IImages(Iterable<String> versions) throws NoImageException {
            for (String version : versions) {
                image(null, version, kafkaConnectS2iImages, ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_S2I_IMAGES);
            }
        }

        /**
         * The Kafka Connect image to use for a Kafka Mirror Maker cluster.
         * @param image The image given in the CR.
         * @param version The version given in the CR.
         * @return The image to use.
         * @throws InvalidResourceException If no image was given in the CR and the version given
         * was not present in {@link ClusterOperatorConfig#STRIMZI_KAFKA_MIRROR_MAKER_IMAGES}.
         */
        public String kafkaMirrorMakerImage(String image, String version) {
            try {
                return image(image,
                        version,
                        kafkaMirrorMakerImages,
                        ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_IMAGES);
            } catch (NoImageException e) {
                throw asInvalidResourceException(version, e);
            }
        }

        InvalidResourceException asInvalidResourceException(String version, NoImageException e) {
            return new InvalidResourceException("Version " + version + " is not supported. " +
                    "Supported versions are: " + String.join(", ", supportedVersions()) + ".",
                    e);
        }

        /**
         * Validate that the given versions have images present in {@link ClusterOperatorConfig#STRIMZI_KAFKA_MIRROR_MAKER_IMAGES}.
         * @param versions The versions to validate.
         * @throws NoImageException If one of the versions lacks an image.
         */
        public void validateKafkaMirrorMakerImages(Iterable<String> versions) throws NoImageException {
            for (String version : versions) {
                image(null, version, kafkaMirrorMakerImages, ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_IMAGES);
            }
        }

       /**
         * The Kafka MirrorMaker 2.0 image to use for a Kafka MirrorMaker 2.0 cluster.
         * @param image The image given in the CR.
         * @param version The version given in the CR.
         * @return The image to use.
         * @throws InvalidResourceException If no image was given in the CR and the version given
         * was not present in {@link ClusterOperatorConfig#STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES}.
         */
        public String kafkaMirrorMaker2Version(String image, String version) {
            try {
                return image(image,
                        version,
                        kafkaMirrorMaker2Images,
                        ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES);
            } catch (NoImageException e) {
                throw asInvalidResourceException(version, e);
            }
        }

        /**
         * Validate that the given versions have images present in {@link ClusterOperatorConfig#STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES}.
         * @param versions The versions to validate.
         * @throws NoImageException If one of the versions lacks an image.
         */
        public void validateKafkaMirrorMaker2Images(Iterable<String> versions) throws NoImageException {
            for (String version : versions) {
                image(null, version, kafkaMirrorMaker2Images, ClusterOperatorConfig.STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("versions{");
            boolean first = true;
            for (String v : supportedVersions()) {
                if (!first) {
                    sb.append(", ");
                }
                first = false;
                KafkaVersion version = version(v);
                sb.append(v).append("=")
                        .append("{proto: ").append(version.protocolVersion)
                        .append(" msg: ").append(version.messageVersion)
                        .append(" kafka-image: ").append(kafkaImages.get(v))
                        .append(" connect-image: ").append(kafkaConnectImages.get(v))
                        .append(" connects2i-image: ").append(kafkaConnectS2iImages.get(v))
                        .append(" mirrormaker-image: ").append(kafkaMirrorMakerImages.get(v))
                        .append(" mirrormaker2-image: ").append(kafkaMirrorMaker2Images.get(v))
                        .append("}");

            }
            sb.append("}");
            return sb.toString();
        }
    }

    private final String version;
    private final String protocolVersion;
    private final String messageVersion;
    private final String zookeeperVersion;
    private final boolean isDefault;
    private final String unsupportedFeatures;

    @JsonCreator
    public KafkaVersion(@JsonProperty("version") String version,
                        @JsonProperty("protocol") String protocolVersion,
                        @JsonProperty("format") String messageVersion,
                        @JsonProperty("zookeeper") String zookeeperVersion,
                        @JsonProperty("default") boolean isDefault,
                        @JsonProperty("unsupported-features") String unsupportedFeatures) {

        this.version = version;
        this.protocolVersion = protocolVersion;
        this.messageVersion = messageVersion;
        this.zookeeperVersion = zookeeperVersion;
        this.isDefault = isDefault;
        this.unsupportedFeatures = unsupportedFeatures;
    }

    @Override
    public String toString() {
        return "KafkaVersion{" +
                "version='" + version + '\'' +
                ", protocolVersion='" + protocolVersion + '\'' +
                ", messageVersion='" + messageVersion + '\'' +
                ", zookeeperVersion='" + zookeeperVersion + '\'' +
                ", isDefault=" + isDefault +
                ", unsupportedFeatures='" + unsupportedFeatures  + '\'' +
                '}';
    }

    public String version() {
        return version;
    }

    public String protocolVersion() {
        return protocolVersion;
    }

    public String messageVersion() {
        return messageVersion;
    }

    public String zookeeperVersion() {
        return zookeeperVersion;
    }

    public boolean isDefault() {
        return isDefault;
    }

    public String unsupportedFeatures() {
        return unsupportedFeatures;
    }

    @Override
    public int compareTo(KafkaVersion o) {
        return compareDottedVersions(this.version, o.version);
    }

    /** Compares this version to a supplied version string.
     * @param version the version string to be compared
     * @return Zero if the supplied versions matches this version;
     * -1 if this version is less than the supplied version;
     * +1 if this version is greater than the supplied version.
     */
    public int compareVersion(String version) {
        return compareDottedVersions(this.version, version);
    }

    /**
     * Compare two decimal version strings, e.g. 1.10.1 &gt; 1.9.2
     * @param version1 The first version.
     * @param version2 The second version.
     * @return Zero if version1 == version2;
     * -1 if version1 &lt; version2;
     * 1 if version1 &gt; version2.
     */
    public static int compareDottedVersions(String version1, String version2) {
        String[] components = version1.split("\\.");
        String[] otherComponents = version2.split("\\.");
        for (int i = 0; i < Math.min(components.length, otherComponents.length); i++) {
            int x = Integer.parseInt(components[i]);
            int y = Integer.parseInt(otherComponents[i]);
            if (x == y) {
                continue;
            } else if (x < y) {
                return -1;
            } else {
                return 1;
            }
        }
        return components.length - otherComponents.length;
    }

    @Override
    public int hashCode() {
        return version.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaVersion that = (KafkaVersion) o;
        return version.equals(that.version);
    }
}
