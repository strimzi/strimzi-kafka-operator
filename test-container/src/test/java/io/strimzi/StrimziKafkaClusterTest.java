/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi;

import io.strimzi.utils.TestUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class StrimziKafkaClusterTest {

    private static final Logger LOGGER = LogManager.getLogger(StrimziKafkaContainerTest.class);

    private StrimziKafkaCluster systemUnderTest;

    @Test
    void testKafkaClusterStartup() throws IOException, InterruptedException {
        final int numberOfBrokers = 3;
        final int numberOfReplicas = 2;

        Map<String, String> kafkaClusterConfiguration = new HashMap<>();
        kafkaClusterConfiguration.put("zookeeper.connect", "zookeeper:2181");

        systemUnderTest = new StrimziKafkaCluster(StrimziKafkaContainer.getStrimziVersion() + "-kafka-" + StrimziKafkaContainer.getLatestKafkaVersion(),
            numberOfBrokers,
            numberOfReplicas,
            kafkaClusterConfiguration);
        systemUnderTest.start();

        // exercise (fetch the data)
        Container.ExecResult result = this.systemUnderTest.getZookeeper().execInContainer(
            "sh", "-c",
            "bin/zookeeper-shell.sh zookeeper:" + StrimziKafkaContainer.ZOOKEEPER_PORT + " ls /brokers/ids | tail -n 1"
        );

        final String brokers = result.getStdout();
        // verify that all kafka brokers are bind to zookeeper
        assertThat(brokers, notNullValue());
        assertThat(brokers.split(",").length, is(numberOfBrokers));

        LOGGER.info("Brokers are {}", systemUnderTest.getBootstrapServers());

        systemUnderTest.stop();
    }

    @Test
    void testKafkaClusterFunctionality() {
        final int numberOfBrokers = 3;
        final int numberOfReplicas = 2;

        Map<String, String> kafkaClusterConfiguration = new HashMap<>();
        kafkaClusterConfiguration.put("zookeeper.connect", "zookeeper:2181");

        systemUnderTest = new StrimziKafkaCluster(StrimziKafkaContainer.getStrimziVersion() + "-kafka-" + StrimziKafkaContainer.getLatestKafkaVersion(),
            numberOfBrokers,
            numberOfReplicas,
            kafkaClusterConfiguration);
        systemUnderTest.start();

        try (
            AdminClient adminClient = AdminClient.create(ImmutableMap.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers()
            ));

            KafkaProducer<String, String> producer = new KafkaProducer<>(
                ImmutableMap.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers(),
                    ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                ),
                new StringSerializer(),
                new StringSerializer()
            );

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                ImmutableMap.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers(),
                    ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  OffsetResetStrategy.EARLIEST.name().toLowerCase(Locale.ROOT)
                ),
                new StringDeserializer(),
                new StringDeserializer()
            );
        ) {
            final String topicName = "example-topic";
            final String recordKey = "strimzi";
            final String recordValue = "the-best-project-in-the-world";

            Collection<NewTopic> topics = Collections.singletonList(new NewTopic(topicName, numberOfReplicas, (short) numberOfReplicas));
            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

            consumer.subscribe(Collections.singletonList(topicName));

            producer.send(new ProducerRecord<>(topicName, recordKey, recordValue)).get();

            TestUtils.waitFor("Consumer records are present", Duration.ofSeconds(10).toMillis(), Duration.ofMinutes(1).toMillis(),
                () -> {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    if (records.isEmpty()) {
                        return false;
                    }

                    // verify count
                    assertThat(records.count(), is(1));

                    ConsumerRecord consumerRecord = records.records(topicName).iterator().next();

                    // verify content of the record
                    assertThat(consumerRecord.topic(), is(topicName));
                    assertThat(consumerRecord.key(), is(recordKey));
                    assertThat(consumerRecord.value(), is(recordValue));

                    return true;
                });

            consumer.unsubscribe();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

        systemUnderTest.stop();
    }
}
