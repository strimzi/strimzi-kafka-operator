/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.externalClients;

import io.strimzi.systemtest.TestConstants;
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
    private static final Random RANDOM = new Random();

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
            .withClientIdConfig("producer-" + RANDOM.nextInt(Integer.MAX_VALUE));
    }

    private ConsumerProperties.ConsumerPropertiesBuilder getConsumerProperties() {
        return new ConsumerProperties.ConsumerPropertiesBuilder()
            .withNamespaceName(namespaceName)
            .withClusterName(clusterName)
            .withBootstrapServerConfig(getBootstrapServerFromStatus())
            .withKeyDeserializerConfig(StringDeserializer.class)
            .withValueDeserializerConfig(StringDeserializer.class)
            .withClientIdConfig("consumer-" + RANDOM.nextInt(Integer.MAX_VALUE))
            .withAutoOffsetResetConfig(OffsetResetStrategy.EARLIEST)
            .withGroupIdConfig(consumerGroup);
    }

    public int sendMessagesPlain() {
        ProducerProperties producerProperties = this.producerProperties;
        if (producerProperties == null || producerProperties.getProperties().isEmpty()) {
            producerProperties =
                getProducerProperties()
                    .withSecurityProtocol(SecurityProtocol.PLAINTEXT)
                    .withSharedProperties()
                    .build();
        }

        return sendMessages(producerProperties);
    }

    public int sendMessagesTls() {
        String caCertName = this.caCertName == null ?
            KafkaUtils.getKafkaExternalListenerCaCertName(namespaceName, clusterName, listenerName) :
            this.caCertName;

        ProducerProperties producerProperties = this.producerProperties;
        if (producerProperties == null || producerProperties.getProperties().isEmpty()) {
            producerProperties =
                getProducerProperties()
                    .withCaSecretName(caCertName)
                    .withKafkaUsername(kafkaUsername)
                    .withSecurityProtocol(securityProtocol)
                    .withSaslMechanism("")
                    .withSharedProperties()
                    .build();
        }

        return sendMessages(producerProperties);
    }

    public int receiveMessagesPlain() {
        ConsumerProperties consumerProperties = this.consumerProperties;
        if (consumerProperties == null || consumerProperties.getProperties().isEmpty()) {
            consumerProperties =
                getConsumerProperties()
                    .withSecurityProtocol(SecurityProtocol.PLAINTEXT)
                    .withSharedProperties()
                    .build();
        }

        return consumeMessages(consumerProperties);
    }

    public int receiveMessagesTls() {
        String caCertName = this.caCertName == null ?
            KafkaUtils.getKafkaExternalListenerCaCertName(namespaceName, clusterName, listenerName) :
            this.caCertName;

        ConsumerProperties consumerProperties = this.consumerProperties;
        if (consumerProperties == null || consumerProperties.getProperties().isEmpty()) {
            consumerProperties =
                getConsumerProperties()
                    .withSecurityProtocol(securityProtocol)
                    .withCaSecretName(caCertName)
                    .withKafkaUsername(kafkaUsername)
                    .withSaslMechanism("")
                    .withSharedProperties()
                    .build();
        }

        return consumeMessages(consumerProperties);
    }

    private int sendMessages(ProducerProperties properties) {
        Producer<String, String> producer = new KafkaProducer<>(properties.getProperties());
        int[] messagesSentCounter = {0};

        CompletableFuture<Integer> sent = new CompletableFuture<>();

        Runnable send = new Runnable() {
            @Override
            public void run() {
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, partition, null, "\"Hello-world - " + messagesSentCounter[0] + "\"");
                try {
                    if (messagesSentCounter[0] == messageCount) {
                        sent.complete(messagesSentCounter[0]);
                    } else {
                        RecordMetadata metadata = producer.send(record).get();
                        LOGGER.debug("Message " + record.value() + " written on Topic=" + metadata.topic() +
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
            int messagesSent = sent.get(TestConstants.GLOBAL_CLIENTS_TIMEOUT, TimeUnit.MILLISECONDS);
            LOGGER.info("Sent {} messages", messagesSent);

            producer.close();

            return messagesSent;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            producer.close();

            e.printStackTrace();
            throw new WaitException(e);
        }
    }

    private int consumeMessages(ConsumerProperties properties) {
        Consumer<String, String> consumer = new KafkaConsumer<>(properties.getProperties());
        consumer.subscribe(Collections.singletonList(topicName));

        CompletableFuture<Integer> received = new CompletableFuture<>();

        int[] size = {0};
        long currentTimeMs = System.currentTimeMillis();

        Runnable poll = new Runnable() {
            @Override
            public void run() {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(15000));

                size[0] += records.count();

                records.forEach(record -> LOGGER.debug("Received message: {}", record.value()));

                if (size[0] >= messageCount) {
                    received.complete(size[0]);
                } else if (System.currentTimeMillis() - currentTimeMs > TestConstants.GLOBAL_TIMEOUT) {
                    received.completeExceptionally(new WaitException("Timeout for polling all the messages"));
                } else {
                    this.run();
                }
            }
        };

        poll.run();

        try {
            int messagesReceived = received.get(TestConstants.GLOBAL_CLIENTS_TIMEOUT, TimeUnit.MILLISECONDS);
            LOGGER.info("Received {} messages", messagesReceived);

            consumer.close();

            return messagesReceived;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            consumer.close();

            e.printStackTrace();
            throw new WaitException(e);
        }
    }
}
