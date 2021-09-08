/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class StrimziKafkaContainerTest {

    private static final Logger LOGGER = LogManager.getLogger(StrimziKafkaContainerTest.class);

    private StrimziKafkaContainer systemUnderTest;

    @Test
    void testAtLeastOneVersionKafkaIsPresent() {
        assumeDocker();
        systemUnderTest = StrimziKafkaContainer.create(1);

        LOGGER.info("Verifying that at least one kafka version is present.");

        assertThat(StrimziKafkaContainer.getSupportedKafkaVersions(), is(not(nullValue())));

        systemUnderTest.stop();
    }

    private void assumeDocker() {
        Assumptions.assumeTrue(System.getenv("DOCKER_CMD") == null || "docker".equals(System.getenv("DOCKER_CMD")));
    }

    @Test
    void testVersions() {
        assumeDocker();
        systemUnderTest = StrimziKafkaContainer.create(1);

        List<String> supportedKafkaVersions = new ArrayList<>();


        // Read Kafka versions
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader("src/main/resources/kafka-versions.txt"))) {
            String kafkaVersion;

            while ((kafkaVersion = bufferedReader.readLine()) != null) {
                supportedKafkaVersions.add(kafkaVersion);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // sort kafka version from low to high
        Collections.sort(supportedKafkaVersions);

        LOGGER.info("This is all supported Kafka versions {}", supportedKafkaVersions.toString());
        assertThat(supportedKafkaVersions, is(StrimziKafkaContainer.getSupportedKafkaVersions()));

        LOGGER.info("Verifying that {} is latest kafka version", supportedKafkaVersions.get(supportedKafkaVersions.size() - 1));
        assertThat(supportedKafkaVersions.get(supportedKafkaVersions.size() - 1), is(StrimziKafkaContainer.getLatestKafkaVersion()));

        // Read Strimzi version
        String strimziVersion = null;
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader("src/main/resources/strimzi-version.txt"))) {
            strimziVersion = bufferedReader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        LOGGER.info("Asserting Strimzi version: {}", strimziVersion);
        assertThat(strimziVersion, is(StrimziKafkaContainer.getStrimziVersion()));

        systemUnderTest.stop();
    }

    @Test
    void testStartContainerWithEmptyConfiguration() {
        assumeDocker();
        systemUnderTest = StrimziKafkaContainer.create(1);

        systemUnderTest.start();

        assertThat(systemUnderTest.getBootstrapServers(), is("PLAINTEXT://localhost:" + systemUnderTest.getMappedPort(9092)));
    }

    @Test
    void testStartContainerWithSomeConfiguration() {
        assumeDocker();

        Map<String, String> kafkaConfiguration = new HashMap<>();

        kafkaConfiguration.put("log.cleaner.enable", "false");
        kafkaConfiguration.put("log.cleaner.backoff.ms", "1000");
        kafkaConfiguration.put("ssl.enabled.protocols", "TLSv1");
        kafkaConfiguration.put("log.index.interval.bytes", "2048");

        systemUnderTest = StrimziKafkaContainer.createWithAdditionalConfiguration(1, kafkaConfiguration);

        systemUnderTest.start();

        String logsFromKafka = systemUnderTest.getLogs();

        assertThat(logsFromKafka, containsString("log.cleaner.enable = false"));
        assertThat(logsFromKafka, containsString("log.cleaner.backoff.ms = 1000"));
        assertThat(logsFromKafka, containsString("ssl.enabled.protocols = [TLSv1]"));
        assertThat(logsFromKafka, containsString("log.index.interval.bytes = 2048"));

        systemUnderTest.stop();
    }
}
