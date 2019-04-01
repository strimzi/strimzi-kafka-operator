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


public class Producer<T> extends ClientHandlerBase<Integer> {
    private static final Logger LOGGER = LogManager.getLogger(Producer.class);
    private Properties properties;
    private final AtomicInteger numSent = new AtomicInteger(0);
    private final String topic;

    public Producer(Properties properties, CompletableFuture<Integer> resultPromise, int messageCount, String topic) {
        super(resultPromise, messageCount);
        this.properties = properties;
        this.topic = topic;
        LOGGER.info("creating producer");
    }

    @Override
    protected void handleClient() {
        KafkaProducer<String, String> producer = KafkaProducer.create(vertx, properties, String.class, String.class);
        sendNext(producer, topic);
    }

    private void sendNext(KafkaProducer<String, String> producer, String topic) {
        if (numSent.get() < messageCount) {

            KafkaProducerRecord<String, String> record =
                    KafkaProducerRecord.create(topic, "message_" + numSent.get());

            producer.write(record, done -> {
                if (done.succeeded()) {
                    RecordMetadata recordMetadata = done.result();
                    LOGGER.info("Message " + record.value() + " written on topic=" + recordMetadata.getTopic() +
                            ", partition=" + recordMetadata.getPartition() +
                            ", offset=" + recordMetadata.getOffset());

                    numSent.getAndIncrement();
                    if (numSent.get() == messageCount) {
                        LOGGER.info("Producer sent {} messages", numSent.get());
                        resultPromise.complete(numSent.get());
                    } else {
                        sendNext(producer, topic);
                    }

                } else {
                    LOGGER.info("Producer didn't produce any message");
                    resultPromise.completeExceptionally(done.cause());
                }
            });

        }
    }
}


//            while(numSent.get() < messageCount) {
//        producer.write(record, done -> {
//        if (done.succeeded()) {
//        RecordMetadata recordMetadata = done.result();
//        LOGGER.info("Message " + record.value() + " written on topic=" + recordMetadata.getTopic() +
//        ", partition=" + recordMetadata.getPartition() +
//        ", offset=" + recordMetadata.getOffset());
//
//        numSent.getAndIncrement();
//        if (numSent.get() == messageCount) {
//        LOGGER.info("Producer sent {} messages", numSent.get());
//        resultPromise.complete(numSent.get());
//        }
//        } else {
//        LOGGER.info("Producer didn't produce any message");
//        resultPromise.completeExceptionally(done.cause());
//        }
//        });
//        }