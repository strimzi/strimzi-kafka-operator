/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.clients;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaProducer {

    private String topic;
    private String bootstrapServers;

    public KafkaProducer() {
    }

    public KafkaProducer(String topic, String bootstrapServers) {
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    private Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaTestProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }

    public List<String> runProducer(final int sendMessageCount) {
        final Producer<Long, String> producer = createProducer();
        long time = System.currentTimeMillis();
        List<String> messagesForTest = new ArrayList<>();
        try {
            for (long index = time; index < time + sendMessageCount; index++) {
                final ProducerRecord<Long, String> record = new
                    ProducerRecord<>(topic, index, "Test message " + index);
                producer.send(record);
            }
        } finally {
            producer.flush();
            producer.close();
        }
        return messagesForTest;
    }

}