/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.libClient;

import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;


public class Consumer extends ClientHandlerBase<Integer> {
    private static final Logger LOGGER = LogManager.getLogger(Consumer.class);
    private Properties properties;
    private final AtomicInteger numReceived = new AtomicInteger(0);
    private final String topic;

    Consumer(Properties properties, CompletableFuture<Integer> resultPromise, int messageCount, String topic) {
        super(resultPromise, messageCount);
        this.properties = properties;
        this.topic = topic;
    }

    @Override
    protected void handleClient() {
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, properties);

        consumer.subscribe(topic, ar -> {
            if (ar.succeeded()) {
                consumer.handler(record -> {
                    LOGGER.debug("Processing key=" + record.key() + ",value=" + record.value() +
                            ",partition=" + record.partition() + ",offset=" + record.offset());
                    numReceived.getAndIncrement();
                    if (numReceived.get() == messageCount) {
                        LOGGER.info("Consumer received {} messages", numReceived.get());
                        resultPromise.complete(numReceived.get());
                    }
                });
            } else {
                LOGGER.info("Consumer could not subscribe " + ar.cause().getMessage());
                resultPromise.completeExceptionally(ar.cause());
            }
        });

    }
}
