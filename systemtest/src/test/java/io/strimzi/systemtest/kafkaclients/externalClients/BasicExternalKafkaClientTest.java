/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.externalClients;

import io.strimzi.StrimziKafkaContainer;
import io.strimzi.systemtest.kafkaclients.clientproperties.ProducerProperties;
import io.strimzi.test.WaitException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntPredicate;

import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

@Tag(EXTERNAL_CLIENTS_USED)
class BasicExternalKafkaClientTest {

    private static final Logger LOGGER = LogManager.getLogger(BasicExternalKafkaClient.class);

    private static final StrimziKafkaContainer STRIMZI_CONTAINER;
    private static final AdminClient ADMIN_CLIENT;

    private static final String TOPIC_NAME = "my-topic";
    private static final int MESSAGE_COUNT = 500;

    static {
        STRIMZI_CONTAINER = new StrimziKafkaContainer();
        STRIMZI_CONTAINER.start();

        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, STRIMZI_CONTAINER.getBootstrapServers());

        ADMIN_CLIENT = AdminClient.create(properties);
    }

    @Test
    void testBasicClientProducerPlainCommunication() {
        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withNamespaceName(".")
            .withClusterName(".")
            .withProducerProperties(
                new ProducerProperties.ProducerPropertiesBuilder()
                    .withNamespaceName(".")
                    .withClusterName(".")
                    .withKeySerializerConfig(StringSerializer.class)
                    .withValueSerializerConfig(StringSerializer.class)
                    .withClientIdConfig("producer-plain-" + new Random().nextInt(Integer.MAX_VALUE))
                    .withSecurityProtocol(SecurityProtocol.PLAINTEXT)
                    .withBootstrapServerConfig(STRIMZI_CONTAINER.getBootstrapServers())
                    .withSharedProperties()
                    .build()
            )
            .build();

        int sent = basicExternalKafkaClient.sendMessagesPlain();
        LOGGER.info("Sent {} messages", sent);
        assertThat(sent, is(MESSAGE_COUNT));
    }

    @Test
    void testSimplePlain() {

        String clientId = "producer-plain-" + new Random().nextInt(Integer.MAX_VALUE);

        ProducerProperties properties = new ProducerProperties.ProducerPropertiesBuilder()
            .withKeySerializerConfig(StringSerializer.class)
            .withValueSerializerConfig(StringSerializer.class)
            .withClientIdConfig(clientId)
            .withSecurityProtocol(SecurityProtocol.PLAINTEXT)
            .withBootstrapServerConfig(STRIMZI_CONTAINER.getBootstrapServers())
            .withSharedProperties()
            .build();

        StringWriter writer = new StringWriter();
        properties.getProperties().list(new PrintWriter(writer));
        LOGGER.info(writer.getBuffer().toString());

        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();
        IntPredicate msgCntPredicate = x -> x == MESSAGE_COUNT;

        try (Producer plainProducer = new Producer(properties, resultPromise, msgCntPredicate, TOPIC_NAME, clientId)) {

            plainProducer.getVertx().deployVerticle(plainProducer);

            plainProducer.getResultPromise().get(Duration.ofSeconds(30).toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw new WaitException(e);
        }
    }

    @Test
    void testBasicClientInParallel() {

        String clientId = "producer-plain-" + new Random().nextInt(Integer.MAX_VALUE);

        ProducerProperties properties = new ProducerProperties.ProducerPropertiesBuilder()
            .withKeySerializerConfig(StringSerializer.class)
            .withValueSerializerConfig(StringSerializer.class)
            .withClientIdConfig(clientId)
            .withSecurityProtocol(SecurityProtocol.PLAINTEXT)
            .withBootstrapServerConfig(STRIMZI_CONTAINER.getBootstrapServers())
            .withSharedProperties()
            .build();

        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();
        IntPredicate msgCntPredicate = x -> x == MESSAGE_COUNT * 20;

        try (Producer plainProducer = new Producer(properties, resultPromise, msgCntPredicate, TOPIC_NAME, clientId)) {

            for (int i = 0; i < 10; i++) {
                plainProducer.setClientName("producer-plain-" + new Random().nextInt(Integer.MAX_VALUE));
                plainProducer.getVertx().deployVerticle(plainProducer);
            }

            plainProducer.getResultPromise().get(Duration.ofSeconds(30).toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            throw new WaitException(e);
        }
    }

    @Test
    void testImmutability() {

        String clientId = "producer-plain-" + new Random().nextInt(Integer.MAX_VALUE);

        ProducerProperties producerProperties = new ProducerProperties.ProducerPropertiesBuilder()
            .withNamespaceName("some-namespace")
            .withClusterName("some-clustername")
            .withKeySerializerConfig(StringSerializer.class)
            .withValueSerializerConfig(StringSerializer.class)
            .withClientIdConfig(clientId)
            .withSecurityProtocol(SecurityProtocol.PLAINTEXT)
            .withBootstrapServerConfig(STRIMZI_CONTAINER.getBootstrapServers())
            .withSharedProperties()
            .build();

        assertThat(producerProperties.getProperties().getProperty(ProducerConfig.CLIENT_ID_CONFIG), is(clientId));

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withNamespaceName(".")
            .withClusterName(".")
            .withProducerProperties(producerProperties.toBuilder(producerProperties).withClientIdConfig("new-one").build())
            .build();

        assertThat(basicExternalKafkaClient.getProducerProperties().getProperties().getProperty(ProducerConfig.CLIENT_ID_CONFIG), is("new-one"));

        LOGGER.info("============1===========");
        StringWriter writer = new StringWriter();
        basicExternalKafkaClient.getProducerProperties().getProperties().list(new PrintWriter(writer));
        LOGGER.info(writer.getBuffer().toString());
        LOGGER.info("============1===========");


        int sent = basicExternalKafkaClient.sendMessagesPlain();
        LOGGER.info("Sent {} messages", sent);
        assertThat(sent, is(MESSAGE_COUNT));

        BasicExternalKafkaClient anotherClientBasedOnTheCurrentOne = basicExternalKafkaClient.toBuilder(basicExternalKafkaClient)
            .withTopicName(TOPIC_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withClusterName("cluster-name-1")
            .withSecurityProtocol(SecurityProtocol.PLAINTEXT)
            .withPartition(1)
            .withProducerProperties(producerProperties.toBuilder(producerProperties).withClientIdConfig("new-kafka-client-id").build())
            .build();

        assertThat(anotherClientBasedOnTheCurrentOne.getTopicName(), is(TOPIC_NAME));
        assertThat(anotherClientBasedOnTheCurrentOne.getMessageCount(), is(MESSAGE_COUNT));
        assertThat(anotherClientBasedOnTheCurrentOne.getClusterName(), is("cluster-name-1"));
        assertThat(anotherClientBasedOnTheCurrentOne.getSecurityProtocol(), is(SecurityProtocol.PLAINTEXT));
        assertThat(anotherClientBasedOnTheCurrentOne.getPartition(), is(1));
        assertThat(anotherClientBasedOnTheCurrentOne.getProducerProperties().getProperties().getProperty(ProducerConfig.CLIENT_ID_CONFIG), is("new-kafka-client-id"));
    }
}
