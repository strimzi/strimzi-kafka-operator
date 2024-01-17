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
import io.strimzi.operator.common.model.InvalidResourceException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
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

        List<KafkaVersion> kafkaVersions = mapper.readValue(reader, new TypeReference<>() { });

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

    /**
     * Lookup class for looking-up Kafka versions and the container images belonging to them
     */
    public static class Lookup {
        private static final String KAFKA_VERSIONS_RESOURCE = "kafka-versions.yaml";
        private final Map<String, KafkaVersion> map;
        private final KafkaVersion defaultVersion;
        private final Map<String, String> kafkaImages;
        private final Map<String, String> kafkaConnectImages;
        private final Map<String, String> kafkaMirrorMakerImages;
        private final Map<String, String> kafkaMirrorMaker2Images;

        /**
         * Constructor
         *
         * @param kafkaImages               Map with container images for various Kafka versions to be used for Kafka brokers and ZooKeeper
         * @param kafkaConnectImages        Map with container images for various Kafka versions to be used for Kafka Connect
         * @param kafkaMirrorMakerImages    Map with container images for various Kafka versions to be used for Kafka Mirror Maker
         * @param kafkaMirrorMaker2Images   Map with container images for various Kafka versions to be used for Kafka Mirror Maker 2
         */
        public Lookup(Map<String, String> kafkaImages,
                      Map<String, String> kafkaConnectImages,
                      Map<String, String> kafkaMirrorMakerImages,
                      Map<String, String> kafkaMirrorMaker2Images) {
            this(new InputStreamReader(
                    KafkaVersion.class.getResourceAsStream("/" + KAFKA_VERSIONS_RESOURCE),
                    StandardCharsets.UTF_8),
                    kafkaImages, kafkaConnectImages, kafkaMirrorMakerImages, kafkaMirrorMaker2Images);
        }

        protected Lookup(Reader reader,
                         Map<String, String> kafkaImages,
                         Map<String, String> kafkaConnectImages,
                         Map<String, String> kafkaMirrorMakerImages,
                         Map<String, String> kafkaMirrorMaker2Images) {
            map = new HashMap<>();
            try {
                defaultVersion = parseKafkaVersions(reader, map);
            } catch (Exception e) {
                throw new RuntimeException("Error reading " + KAFKA_VERSIONS_RESOURCE, e);
            }
            this.kafkaImages = kafkaImages;
            this.kafkaConnectImages = kafkaConnectImages;
            this.kafkaMirrorMakerImages = kafkaMirrorMakerImages;
            this.kafkaMirrorMaker2Images = kafkaMirrorMaker2Images;
        }

        /**
         * @return  The default Kafka version
         */
        public KafkaVersion defaultVersion() {
            return defaultVersion;
        }

        /**
         * Find the version from the given version string. This method looks through all known versions and is used
         * during upgrades to check any Kafka versions not supported anymore but known to the operator (and supported in
         * the previous versions).
         *
         * KafkaUpgradeException is thrown if the version passed as argument is not found among the known versions.
         *
         * @param version The version.
         *
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

        /**
         * Find the version from the given version string. This method looks only through supported versions and is used
         * to validate the versions used in custom resources.
         *
         * UnsupportedKafkaVersionException is thrown if the version passed as argument is not found or is not supported.
         *
         * @param version The version.
         *
         * @return The KafkaVersion.
         */
        public KafkaVersion supportedVersion(String version) {
            KafkaVersion result;

            if (version == null) {
                result = defaultVersion;
            } else {
                result = map.get(version);
            }

            if (result == null || !result.isSupported()) {
                throw new UnsupportedKafkaVersionException(String.format(
                        "Unsupported Kafka.spec.kafka.version: %s. Supported versions are: %s", version, supportedVersions()));
            }

            return result;
        }

        /**
         * @return  Set of supported Kafka versions
         */
        public Set<String> supportedVersions() {
            return map.keySet().stream().filter(version -> map.get(version).isSupported).collect(Collectors.toCollection(TreeSet::new));
        }

        /**
         * Finds Kafka versions which support some feature
         *
         * @param feature   Feature which we want to be supported
         *
         * @return  Set of Kafka versions which support a particular feature
         */
        public Set<String> supportedVersionsForFeature(String feature) {
            return map.entrySet().stream()
                    .filter(version -> version.getValue().isSupported)
                    .filter(entry -> entry.getValue().unsupportedFeatures() == null || !entry.getValue().unsupportedFeatures().contains(feature))
                    .map(Entry::getKey)
                    .collect(Collectors.toSet());
        }

        private String image(final String crImage, final String crVersion, Map<String, String> images)
                throws NoImageException {
            final String image;
            if (crImage == null) {
                if (crVersion == null) {
                    image = images.get(defaultVersion().version());
                    if (image == null) {
                        throw new NoImageException("No image for default version " + defaultVersion() + " in " + images);
                    }
                } else {
                    image = images.get(crVersion);
                    if (image == null) {
                        throw new NoImageException("No image for version " + crVersion + " in " + images);
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
                return image(image, version, kafkaImages);
            } catch (NoImageException e) {
                throw asInvalidResourceException(version, e);
            }
        }

        /**
         * Validates whether each supported version has configured image and whether each configured image matches
         * supported Kafka version.
         *
         * @param versions The versions to validate.
         * @param images Map with configured images
         * @throws NoImageException If one of the versions lacks an image.
         * @throws UnsupportedVersionException If any version with configured image is not supported
         */
        public void validateImages(Set<String> versions, Map<String, String> images) throws NoImageException, UnsupportedVersionException   {
            for (String version : versions) {
                image(null, version, images);
            }

            for (String version : images.keySet())  {
                if (!versions.contains(version)) {
                    throw new UnsupportedVersionException("Kafka version " + version + " has a container image configured but is not supported.");
                }
            }
        }

        /**
         * Validate that the given versions have images present in {@link ClusterOperatorConfig#STRIMZI_KAFKA_IMAGES}.
         * @param versions The versions to validate.
         * @throws NoImageException If one of the versions lacks an image.
         * @throws UnsupportedVersionException If any version with configured image is not supported
         */
        public void validateKafkaImages(Set<String> versions) throws NoImageException, UnsupportedVersionException {
            validateImages(versions, kafkaImages);
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
                        kafkaConnectImages);
            } catch (NoImageException e) {
                throw asInvalidResourceException(version, e);
            }
        }

        /**
         * Validate that the given versions have images present in {@link ClusterOperatorConfig#STRIMZI_KAFKA_CONNECT_IMAGES}.
         * @param versions The versions to validate.
         * @throws NoImageException If one of the versions lacks an image.
         * @throws UnsupportedVersionException If any version with configured image is not supported
         */
        public void validateKafkaConnectImages(Set<String> versions) throws NoImageException, UnsupportedVersionException {
            validateImages(versions, kafkaConnectImages);
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
                        kafkaMirrorMakerImages);
            } catch (NoImageException e) {
                throw asInvalidResourceException(version, e);
            }
        }

        InvalidResourceException asInvalidResourceException(String version, NoImageException e) {
            return new InvalidResourceException("Kafka version " + version + " is not supported. " +
                    "Supported versions are: " + String.join(", ", supportedVersions()) + ".",
                    e);
        }

        /**
         * Validate that the given versions have images present in {@link ClusterOperatorConfig#STRIMZI_KAFKA_MIRROR_MAKER_IMAGES}.
         * @param versions The versions to validate.
         * @throws NoImageException If one of the versions lacks an image.
         * @throws UnsupportedVersionException If any version with configured image is not supported
         */
        public void validateKafkaMirrorMakerImages(Set<String> versions) throws NoImageException, UnsupportedVersionException {
            validateImages(versions, kafkaMirrorMakerImages);
        }

       /**
         * The Kafka MirrorMaker 2 image to use for a Kafka MirrorMaker 2 cluster.
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
                        kafkaMirrorMaker2Images);
            } catch (NoImageException e) {
                throw asInvalidResourceException(version, e);
            }
        }

        /**
         * Validate that the given versions have images present in {@link ClusterOperatorConfig#STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES}.
         * @param versions The versions to validate.
         * @throws NoImageException If one of the versions lacks an image.
         * @throws UnsupportedVersionException If any version with configured image is not supported
         */
        public void validateKafkaMirrorMaker2Images(Set<String> versions) throws NoImageException, UnsupportedVersionException {
            validateImages(versions, kafkaMirrorMaker2Images);
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
                        .append(" mirrormaker-image: ").append(kafkaMirrorMakerImages.get(v))
                        .append(" mirrormaker2-image: ").append(kafkaMirrorMaker2Images.get(v))
                        .append("}");

            }
            sb.append("}");
            return sb.toString();
        }
    }

    /**
     * Thrown to indicate that given Kafka version is not valid or not supported.
     */
    public static class UnsupportedKafkaVersionException extends InvalidResourceException {
        /**
         * Constructor
         *
         * @param s     The error message
         */
        public UnsupportedKafkaVersionException(String s) {
            super(s);
        }
    }

    private final String version;
    private final String protocolVersion;
    private final String messageVersion;
    private final String metadataVersion;
    private final String zookeeperVersion;
    private final boolean isDefault;
    private final boolean isSupported;
    private final String unsupportedFeatures;

    /**
     * Class describing a Kafka version. This is used to deserialize the YAML file with the Kafka versions
     *
     * @param version               Kafka version
     * @param protocolVersion       Inter-broker protocol version
     * @param messageVersion        Log message format version
     * @param metadataVersion       KRaft Metadata version
     * @param zookeeperVersion      ZooKeeper version
     * @param isDefault             Flag indicating if this Kafka version is default
     * @param isSupported           Flag indicating if this Kafka version is supported by this operator version
     * @param unsupportedFeatures   Unsupported features
     */
    @JsonCreator
    public KafkaVersion(@JsonProperty("version") String version,
                        @JsonProperty("protocol") String protocolVersion,
                        @JsonProperty("format") String messageVersion,
                        @JsonProperty("metadata") String metadataVersion,
                        @JsonProperty("zookeeper") String zookeeperVersion,
                        @JsonProperty("default") boolean isDefault,
                        @JsonProperty("supported") boolean isSupported,
                        @JsonProperty("unsupported-features") String unsupportedFeatures) {

        this.version = version;
        this.protocolVersion = protocolVersion;
        this.messageVersion = messageVersion;
        this.zookeeperVersion = zookeeperVersion;
        this.metadataVersion = metadataVersion;
        this.isDefault = isDefault;
        this.isSupported = isSupported;
        this.unsupportedFeatures = unsupportedFeatures;
    }

    @Override
    public String toString() {
        return "KafkaVersion{" +
                "version='" + version + '\'' +
                ", protocolVersion='" + protocolVersion + '\'' +
                ", messageVersion='" + messageVersion + '\'' +
                ", zookeeperVersion='" + zookeeperVersion + '\'' +
                ", metadataVersion='" + metadataVersion + '\'' +
                ", isDefault=" + isDefault +
                ", isSupported=" + isSupported +
                ", unsupportedFeatures='" + unsupportedFeatures  + '\'' +
                '}';
    }

    /**
     * @return  Kafka version
     */
    public String version() {
        return version;
    }

    /**
     * @return  Inter-broker protocol version
     */
    public String protocolVersion() {
        return protocolVersion;
    }

    /**
     * @return  Log message format version
     */
    public String messageVersion() {
        return messageVersion;
    }

    /**
     * @return  KRaft metadata version
     */
    public String metadataVersion() {
        return metadataVersion;
    }

    /**
     * @return  ZooKeeper version
     */
    public String zookeeperVersion() {
        return zookeeperVersion;
    }

    /**
     * @return  True if this Kafka version is the default. False otherwise.
     */
    public boolean isDefault() {
        return isDefault;
    }

    /**
     * @return  True if this Kafka version is supported. False otherwise.
     */
    public boolean isSupported() {
        return isSupported;
    }

    /**
     * @return  Unsupported Kafka versions
     */
    public String unsupportedFeatures() {
        return unsupportedFeatures;
    }

    @Override
    public int compareTo(KafkaVersion o) {
        return compareDottedVersions(this.version, o.version);
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
            int compared = Integer.compare(x, y);
            if (compared != 0) {
                return compared;
            }
        }
        // mismatch was not found, but the versions are of different length, e.g. 2.8 and 2.8.0
        return 0;
    }

    /**
     * Compare two decimal version strings, e.g. 3.0 &gt; 2.7-IV1. Ignores the IV part
     * @param version1 The first version.
     * @param version2 The second version.
     * @return Zero if version1 == version2;
     * -1 if version1 &lt; version2;
     * 1 if version1 &gt; version2.
     */
    public static int compareDottedIVVersions(String version1, String version2) {
        // Due to validation in KafkaConfiguration.validate only values like 2.8 or 2.8-IV0 can get there
        String trimmedVersion1 = version1.split("-")[0];
        String trimmedVersion2 = version2.split("-")[0];
        return compareDottedVersions(trimmedVersion1, trimmedVersion2);
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
