/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.clients;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KafkaConsumer {

    private String topic;
    private String bootstrapServers;

    public KafkaConsumer(String topic, String bootstrapServers) {
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;
    }

    public KafkaConsumer() {
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    private Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "KafkaTestConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");
        final Consumer<Long, String> consumer =
                new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    public List<String> runConsumer() {
        final Consumer<Long, String> consumer = createConsumer();
        List<String> consumedMessages = new ArrayList<>();
        final int giveUp = 50;
        int noRecordsCount = 0;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(100);
            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }
            consumerRecords.forEach(record -> {
                consumedMessages.add(record.value());
            });
        }
        consumer.close();
        System.out.printf("Consumed %d massages", consumedMessages.size());
        return consumedMessages;
    }
}