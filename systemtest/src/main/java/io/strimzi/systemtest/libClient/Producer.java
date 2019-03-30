/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.libClient;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;


public class Producer<T> extends ClientHandlerBase<Integer> {
    private static final Logger LOGGER = LogManager.getLogger(Producer.class);
    private Properties properties;
    private int messageCount;
    private Predicate predicate;
    private final AtomicInteger numSent = new AtomicInteger(0);

    public Producer(Properties properties, String containerId, CompletableFuture<Integer> resultPromise, int messageCount) {
        super(containerId, resultPromise);
        this.properties = properties;
        this.messageCount = messageCount;
//        this.predicate = predicate;
    }

    @Override
    protected void produceMessages() {
        KafkaProducer<String, String> producer = KafkaProducer.create(vertx, properties, String.class, String.class);

        sendNext(producer);
    }

    private void sendNext(KafkaProducer<String, String> producer) {
        LOGGER.info("sendNext");
        if (numSent.get() < messageCount) {
            // only topic and message value are specified, round robin on destination partitions
            KafkaProducerRecord<String, String> record =
                    KafkaProducerRecord.create("test", "message_" + numSent.get());

            producer.write(record, done -> {
                if (done.succeeded()) {
                    RecordMetadata recordMetadata = done.result();
                    LOGGER.info("Message " + record.value() + " written on topic=" + recordMetadata.getTopic() +
                            ", partition=" + recordMetadata.getPartition() +
                            ", offset=" + recordMetadata.getOffset());

                    numSent.getAndIncrement();
                    if (numSent.get() == messageCount) {
                        resultPromise.complete(numSent.get());
                    } else {
                        sendNext(producer);
                    }

                } else {
                    resultPromise.completeExceptionally(done.cause());
                }
            });
        }
    }
}
