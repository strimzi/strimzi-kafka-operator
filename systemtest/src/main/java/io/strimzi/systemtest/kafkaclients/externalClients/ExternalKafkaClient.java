/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.externalClients;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.AbstractKafkaClient;
import io.strimzi.systemtest.kafkaclients.clientproperties.ConsumerProperties;
import io.strimzi.systemtest.kafkaclients.clientproperties.ProducerProperties;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.test.WaitException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ExternalKafkaClient extends AbstractKafkaClient<ExternalKafkaClient.Builder> {

    private static final Logger LOGGER = LogManager.getLogger(ExternalKafkaClient.class);

    protected ExternalKafkaClient(AbstractKafkaClient.Builder<ExternalKafkaClient.Builder> builder) {
        super(builder);
    }

    public static class Builder extends AbstractKafkaClient.Builder<ExternalKafkaClient.Builder> {
        @Override
        public ExternalKafkaClient build() {
            return new ExternalKafkaClient(this);
        }
    }

    @Override
    protected Builder newBuilder() {
        return new Builder();
    }

    @Override
    public ExternalKafkaClient.Builder toBuilder() {
        return (ExternalKafkaClient.Builder) super.toBuilder();
    }

    private ProducerProperties.ProducerPropertiesBuilder getProducerProperties() {
        return new ProducerProperties.ProducerPropertiesBuilder()
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withBootstrapServerConfig(getBootstrapServerFromStatus())
            .withKeySerializerConfig(StringSerializer.class)
            .withValueSerializerConfig(StringSerializer.class)
            .withClientIdConfig("producer-" + new Random().nextInt(Integer.MAX_VALUE));
    }

    private ConsumerProperties.ConsumerPropertiesBuilder getConsumerProperties() {
        return new ConsumerProperties.ConsumerPropertiesBuilder()
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withBootstrapServerConfig(getBootstrapServerFromStatus())
            .withKeyDeserializerConfig(StringDeserializer.class)
            .withValueDeserializerConfig(StringDeserializer.class)
            .withClientIdConfig("consumer-" + new Random().nextInt(Integer.MAX_VALUE))
            .withAutoOffsetResetConfig(OffsetResetStrategy.EARLIEST)
            .withGroupIdConfig(consumerGroup);
    }

    public int sendMessagesPlain() {
        if (this.producerProperties == null) {
            this.producerProperties =
                getProducerProperties()
                    .withSecurityProtocol(SecurityProtocol.PLAINTEXT)
                    .withSharedProperties()
                    .build();
        }

        return sendMessages();
    }

    public int sendMessagesTls() {
        this.caCertName = this.caCertName == null ?
            KafkaUtils.getKafkaExternalListenerCaCertName(namespaceName, clusterName, listenerName) :
            this.caCertName;

        if (this.producerProperties == null) {
            this.producerProperties =
                getProducerProperties()
                    .withCaSecretName(caCertName)
                    .withKafkaUsername(kafkaUsername)
                    .withSecurityProtocol(securityProtocol)
                    .withSaslMechanism("")
                    .withSharedProperties()
                    .build();
        }

        return sendMessages();
    }

    public int receiveMessagesPlain() {
        if (this.consumerProperties == null) {
            this.consumerProperties =
                getConsumerProperties()
                    .withSecurityProtocol(SecurityProtocol.PLAINTEXT)
                    .withSharedProperties()
                    .build();
        }

        return consumeMessages();
    }

    public int receiveMessagesTls() {
        this.caCertName = this.caCertName == null ?
            KafkaUtils.getKafkaExternalListenerCaCertName(namespaceName, clusterName, listenerName) :
            this.caCertName;

        if (this.consumerProperties == null) {
            this.consumerProperties =
                getConsumerProperties()
                    .withSecurityProtocol(securityProtocol)
                    .withCaSecretName(caCertName)
                    .withKafkaUsername(kafkaUsername)
                    .withSaslMechanism("")
                    .withSharedProperties()
                    .build();
        }

        return consumeMessages();
    }

    private int sendMessages() {
        Producer<String, String> producer = new KafkaProducer<>(producerProperties.getProperties());
        int[] messagesSentCounter = {0};

        CompletableFuture<Integer> sent = new CompletableFuture<>();

        Runnable send = new Runnable() {
            @Override
            public void run() {
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, partition, null, String.format("Hello-world - %s", messagesSentCounter[0]));
                try {
                    if (messagesSentCounter[0] == messageCount) {
                        sent.complete(messagesSentCounter[0]);
                    } else {
                        RecordMetadata metadata = producer.send(record).get();
                        LOGGER.debug("Message " + record.value() + " written on topic=" + metadata.topic() +
                            ", partition=" + metadata.partition() +
                            ", offset=" + metadata.offset());
                        messagesSentCounter[0]++;
                        this.run();
                    }
                } catch (Exception e) {
                    LOGGER.error("Error sending message {} - {}", messagesSentCounter[0], e.getCause());
                    e.printStackTrace();
                    sent.completeExceptionally(e);
                }
            }
        };

        send.run();

        try {
            int messagesSent = sent.get(Constants.GLOBAL_CLIENTS_TIMEOUT, TimeUnit.MILLISECONDS);
            LOGGER.info("Sent {} messages.", messagesSent);

            producer.close();

            return messagesSent;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            producer.close();

            e.printStackTrace();
            throw new WaitException(e);
        }
    }

    private int consumeMessages() {
        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProperties.getProperties());
        consumer.subscribe(Collections.singletonList(topicName));

        CompletableFuture<Integer> received = new CompletableFuture<>();

        Runnable poll = new Runnable() {
            @Override
            public void run() {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Constants.GLOBAL_CLIENTS_TIMEOUT));

                int size = records.count();

                records.forEach(record -> LOGGER.debug("Received message: {}", record.value()));

                if (size >= messageCount) {
                    received.complete(size);
                } else {
                    this.run();
                }
            }
        };

        poll.run();

        try {
            int messagesReceived = received.get(Constants.GLOBAL_CLIENTS_TIMEOUT, TimeUnit.MILLISECONDS);
            LOGGER.info("Received {} messages.", messagesReceived);

            consumer.close();

            return messagesReceived;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            consumer.close();

            e.printStackTrace();
            throw new WaitException(e);
        }
    }
}
