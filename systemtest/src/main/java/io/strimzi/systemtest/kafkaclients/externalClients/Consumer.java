/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.externalClients;

import io.strimzi.systemtest.kafkaclients.KafkaClientProperties;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;

public class Consumer extends ClientHandlerBase<Integer> implements AutoCloseable {
    private static final Logger LOGGER = LogManager.getLogger(Consumer.class);
    private final KafkaClientProperties properties;
    private final AtomicInteger numReceived = new AtomicInteger(0);
    private final String topic;
    private final String clientName;
    private final KafkaConsumer<String, String> consumer;

    Consumer(KafkaClientProperties properties, CompletableFuture<Integer> resultPromise, IntPredicate msgCntPredicate, String topic, String clientName) {
        super(resultPromise, msgCntPredicate);
        this.properties = properties;
        this.topic = topic;
        this.clientName = clientName;
        this.vertx = Vertx.vertx();
        this.consumer = KafkaConsumer.create(vertx, properties.getProperties());
    }

    @Override
    protected void handleClient() {
        LOGGER.info("Consumer is starting with following properties: {}", properties.getProperties().toString());

        if (msgCntPredicate.test(-1)) {
            vertx.eventBus().consumer(clientName, msg -> {
                if (msg.body().equals("stop")) {
                    LOGGER.debug("Received stop command! Consumed messages: {}", numReceived.get());
                    resultPromise.complete(numReceived.get());
                }
            });
        }

        consumer.subscribe(topic, ar -> {
            if (ar.succeeded()) {
                consumer.handler(record -> {
                    LOGGER.debug("Processing key=" + record.key() + ",value=" + record.value() +
                            ",partition=" + record.partition() + ",offset=" + record.offset());
                    numReceived.getAndIncrement();

                    if (msgCntPredicate.test(numReceived.get())) {
                        LOGGER.info("Consumer consumed {} messages", numReceived.get());
                        resultPromise.complete(numReceived.get());
                    }
                });
            } else {
                LOGGER.warn("Consumer could not subscribe " + ar.cause().getMessage());
                resultPromise.completeExceptionally(ar.cause());
            }
        });
    }

    @Override
    public void close() {
        if (vertx != null) {
            LOGGER.info("Closing Consumer instance {} with client.id {}", consumer.getClass().getName(), properties.getProperties().get(ConsumerConfig.CLIENT_ID_CONFIG));
            consumer.close();

            LOGGER.info("Closing Vert.x instance {}", this.getClass().getName());
            vertx.close();
        }
    }
}
