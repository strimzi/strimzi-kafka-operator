/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.operator.cluster.KafkaUpgradeException;

import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a Kafka version that's supported by this CO
 */
public class KafkaVersion implements Comparable<KafkaVersion> {

    /**
     * Parse the version information present in the {@code /kafka-versions} classpath resource.
     * @param reader A leader for the version info.
     * @param mapOfVersions A map of the versions to add to.
     * @return The default version.
     * @throws Exception
     */
    static KafkaVersion parseKafkaVersions(LineNumberReader reader, Map<String, KafkaVersion> mapOfVersions)
            throws Exception {
        KafkaVersion defaultVersion = null;
        String line = reader.readLine();
        while (line != null) {
            if (!line.isEmpty() && !line.startsWith("#")) {
                Pattern pattern = Pattern.compile(
                        "(?<version>[0-9.]+)\\s+" +
                                "(?<default>default)?\\s+" +
                                "(?<proto>[0-9.]+)\\s+" +
                                "(?<msg>[0-9.]+)\\s+" +
                                "(?<sha>[0-9A-Za-z]+)");
                Matcher matcher = pattern.matcher(line);
                if (matcher.matches()) {
                    String version = matcher.group("version");
                    KafkaVersion kafkaVersion = new KafkaVersion(version,
                            matcher.group("proto"),
                            matcher.group("msg"));
                    if (mapOfVersions.put(version, kafkaVersion) != null) {
                        throw new Exception("Duplicate version '" + version + "' on line " + reader.getLineNumber());
                    }
                    if (matcher.group("default") != null) {
                        if (defaultVersion == null) {
                            defaultVersion = kafkaVersion;
                        } else {
                            throw new Exception("Multiple default versions given");
                        }
                    }
                } else {
                    throw new Exception("Malformed line: " + reader.getLineNumber());
                }
            }
            line = reader.readLine();
        }
        if (defaultVersion == null) {
            throw new Exception("No version was configured as the default");
        }
        return defaultVersion;
    }

    public static class Lookup {
        public static final String KAFKA_VERSIONS_RESOURCE = "kafka-versions";
        private final Map<String, KafkaVersion> map;
        private final KafkaVersion defaultVersion;

        public Lookup() {
            this(new InputStreamReader(
                    KafkaVersion.class.getResourceAsStream("/" + KAFKA_VERSIONS_RESOURCE),
                    StandardCharsets.UTF_8));
        }

        protected Lookup(Reader reader) {
            map = new HashMap<>(5);
            try {
                try (LineNumberReader lnReader = new LineNumberReader(reader)) {
                    defaultVersion = parseKafkaVersions(lnReader, map);
                }
            } catch (Exception e) {
                throw new RuntimeException("Error reading " + KAFKA_VERSIONS_RESOURCE, e);
            }
        }

        public KafkaVersion defaultVersion() {
            return defaultVersion;
        }

        /** Find the version from the given version string */
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
    }

    private final String version;
    private final String protocolVersion;
    private final String messageVersion;

    private KafkaVersion(String version, String protocolVersion, String messageVersion) {
        this.version = version;
        this.protocolVersion = protocolVersion;
        this.messageVersion = messageVersion;
    }

    @Override
    public String toString() {
        return version;
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

    @Override
    public int compareTo(KafkaVersion o) {
        return compareDottedVersions(this.version, o.version);
    }

    /**
     * Compare two decimal version strings, e.g. 1.10.1 &gt; 1.9.2
     * @param version1
     * @param version2
     * @return Zero if version1 == version2;
     * less than 1 if version1 &gt; version2;
     * greater than 1 if version1 &gt; version2.
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
