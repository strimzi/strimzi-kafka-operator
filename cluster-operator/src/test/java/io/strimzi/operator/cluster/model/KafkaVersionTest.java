/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ParallelSuite
public class KafkaVersionTest {
    private static final String KAFKA_VERSIONS_VALID_RESOURCE = "kafka-versions/kafka-versions-valid.yaml";
    private static final String KAFKA_VERSIONS_NODEFAULT_RESOURCE = "kafka-versions/kafka-versions-nodefault.yaml";
    private static final String KAFKA_VERSIONS_TWODEFAULTS_RESOURCE = "kafka-versions/kafka-versions-twodefaults.yaml";
    private static final String KAFKA_VERSIONS_DUPLICATES_RESOURCE = "kafka-versions/kafka-versions-duplicates.yaml";

    private Reader getKafkaVersionsReader(String kafkaVersions) {
        return new InputStreamReader(KafkaVersion.class.getResourceAsStream("/" + kafkaVersions), StandardCharsets.UTF_8);
    }

    @ParallelTest
    public void parsingInvalidVersionTest() {
        KafkaVersion kv = new KafkaVersion("2.8.0", "2.8", "2.8", "3.6.9", false, true, "");
        assertThat(KafkaVersion.compareDottedIVVersions("2.7-IV1", kv.protocolVersion()), lessThan(0));
        assertThat(KafkaVersion.compareDottedIVVersions("2.9-IV1", kv.protocolVersion()), greaterThan(0));

        assertThrows(NumberFormatException.class, () -> KafkaVersion.compareDottedIVVersions("wrong", kv.protocolVersion()));
    }

    @ParallelTest
    public void parsingTest() throws Exception {
        Map<String, KafkaVersion> map = new HashMap<>();
        KafkaVersion defaultVersion = KafkaVersion.parseKafkaVersions(getKafkaVersionsReader(KAFKA_VERSIONS_VALID_RESOURCE), map);

        assertThat(defaultVersion.version(), is("1.2.0"));

        assertThat(map.size(), is(4));

        assertThat(map.containsKey("1.2.0"), is(true));
        assertThat(map.get("1.2.0").version(), is("1.2.0"));
        assertThat(map.get("1.2.0").protocolVersion(), is("1.2"));
        assertThat(map.get("1.2.0").messageVersion(), is("1.2"));
        assertThat(map.get("1.2.0").isSupported(), is(true));

        assertThat(map.containsKey("1.1.0"), is(true));
        assertThat(map.get("1.1.0").version(), is("1.1.0"));
        assertThat(map.get("1.1.0").protocolVersion(), is("1.1"));
        assertThat(map.get("1.1.0").messageVersion(), is("1.1"));
        assertThat(map.get("1.1.0").isSupported(), is(true));

        assertThat(map.containsKey("1.1.1"), is(true));
        assertThat(map.get("1.1.1").version(), is("1.1.1"));
        assertThat(map.get("1.1.1").protocolVersion(), is("1.1"));
        assertThat(map.get("1.1.1").messageVersion(), is("1.1"));
        assertThat(map.get("1.1.1").isSupported(), is(true));

        assertThat(map.containsKey("1.0.0"), is(true));
        assertThat(map.get("1.0.0").version(), is("1.0.0"));
        assertThat(map.get("1.0.0").protocolVersion(), is("1.0"));
        assertThat(map.get("1.0.0").messageVersion(), is("1.0"));
        assertThat(map.get("1.0.0").isSupported(), is(false));
    }

    @ParallelTest
    public void duplicateVersionTest() {
        assertThrows(IllegalArgumentException.class, () -> {
            Map<String, KafkaVersion> map = new HashMap<>();
            KafkaVersion.parseKafkaVersions(getKafkaVersionsReader(KAFKA_VERSIONS_DUPLICATES_RESOURCE), map);
        });
    }

    @ParallelTest
    public void noDefaultTest() {
        assertThrows(RuntimeException.class, () -> {
            Map<String, KafkaVersion> map = new HashMap<>();
            KafkaVersion.parseKafkaVersions(getKafkaVersionsReader(KAFKA_VERSIONS_NODEFAULT_RESOURCE), map);
        });
    }

    @ParallelTest
    public void multipleDefaultTest() {
        assertThrows(IllegalArgumentException.class, () -> {
            Map<String, KafkaVersion> map = new HashMap<>();
            KafkaVersion.parseKafkaVersions(getKafkaVersionsReader(KAFKA_VERSIONS_TWODEFAULTS_RESOURCE), map);
        });
    }

    @ParallelTest
    public void compareEqualVersionMMPTest() {
        assertThat(KafkaVersion.compareDottedVersions("3.0", "3.0.0"), is(0));
        assertThat(KafkaVersion.compareDottedVersions("3.0.0", "3.0"), is(0));
    }

    @ParallelTest
    public void compareEqualVersionTest() {
        assertThat(KafkaVersion.compareDottedVersions(KafkaVersionTestUtils.DEFAULT_KAFKA_VERSION, KafkaVersionTestUtils.DEFAULT_KAFKA_VERSION), is(0));
    }

    @ParallelTest
    public void compareVersionLowerTest() {
        assertThat(KafkaVersion.compareDottedVersions(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION, KafkaVersionTestUtils.LATEST_KAFKA_VERSION), lessThan(0));
    }

    @ParallelTest
    public void compareVersionHigherTest() {
        assertThat(KafkaVersion.compareDottedVersions(KafkaVersionTestUtils.LATEST_KAFKA_VERSION, KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION), greaterThan(0));
    }

}
