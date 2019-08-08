/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.clients.lib;

import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;


public class Consumer extends ClientHandlerBase<Integer> {
    private static final Logger LOGGER = LogManager.getLogger(Consumer.class);
    private Properties properties;
    private final AtomicInteger numReceived = new AtomicInteger(0);
    private final String topic;
    private final String clientName;

    Consumer(Properties properties, CompletableFuture<Integer> resultPromise, IntPredicate msgCntPredicate, String topic, String clientName) {
        super(resultPromise, msgCntPredicate);
        this.properties = properties;
        this.topic = topic;
        this.clientName = clientName;
    }

    @Override
    protected void handleClient() {
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, properties);

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
}
