/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.libClient;

import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;


public class Consumer<T> extends ClientHandlerBase<Integer> {
    private static final Logger LOGGER = LogManager.getLogger(Consumer.class);
    private Properties properties;
    private final AtomicInteger numReceived = new AtomicInteger(0);
    private final String topic;

    public Consumer(Properties properties, CompletableFuture<Integer> resultPromise, int messageCount, String topic) {
        super(resultPromise, messageCount);
        this.properties = properties;
        this.topic = topic;
        LOGGER.info("creating consumer");
    }

    @Override
    protected void handleClient() {
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, properties);


        LOGGER.info("Subscribe");
        consumer.subscribe(topic, ar -> {
            if (ar.succeeded()) {
                LOGGER.info("Create handler");
                consumer.handler(record -> {
                    LOGGER.info("Processing key=" + record.key() + ",value=" + record.value() +
                            ",partition=" + record.partition() + ",offset=" + record.offset());
                    numReceived.getAndIncrement();
                    LOGGER.info("Received {}", numReceived.get());
                    if (numReceived.get() == messageCount) {
                        LOGGER.info("Consumer sent {} messages", numReceived.get());
                        resultPromise.complete(numReceived.get());
                    } else {
                        LOGGER.info("Not yet finished");
                    }
                });
            } else {
                LOGGER.info("Consumer could not subscribe " + ar.cause().getMessage());
                resultPromise.completeExceptionally(ar.cause());
            }
        });

    }

    // Not used, just for test
    private void receiveNext(KafkaConsumer<String, String> consumer, String topic) {
        vertx.setPeriodic(1000, timerId -> {

            consumer.poll(100, ar1 -> {

                if (ar1.succeeded()) {

                    KafkaConsumerRecords<String, String> records = ar1.result();
                    for (int i = 0; i < records.size(); i++) {
                        KafkaConsumerRecord<String, String> record = records.recordAt(i);
                        LOGGER.info("key=" + record.key() + ",value=" + record.value() +
                                ",partition=" + record.partition() + ",offset=" + record.offset());
                    }
                }
            });

        });
    }
}
