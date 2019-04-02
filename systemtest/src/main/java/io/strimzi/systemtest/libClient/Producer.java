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
import java.util.function.IntPredicate;

public class Producer extends ClientHandlerBase<Integer> {
    private static final Logger LOGGER = LogManager.getLogger(Producer.class);
    private Properties properties;
    private final AtomicInteger numSent = new AtomicInteger(0);
    private final String topic;
    private final String clientName;

    Producer(Properties properties, CompletableFuture<Integer> resultPromise, IntPredicate msgCntPredicate, String topic, String clientName) {
        super(resultPromise, msgCntPredicate);
        this.properties = properties;
        this.topic = topic;
        this.clientName = clientName;
    }

    @Override
    protected void handleClient() {
        KafkaProducer<String, String> producer = KafkaProducer.create(vertx, properties);
        LOGGER.info("producer vytvoren");

        if (msgCntPredicate.test(-1)) {
            LOGGER.info("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
            vertx.eventBus().consumer(clientName, msg -> {
                LOGGER.info(msg.body());
                LOGGER.info(msg.address());
                if (msg.body().equals("stop")) {
                    LOGGER.info("Received stop command! Sent: {}", numSent.get());
                    resultPromise.complete(numSent.get());
                }
            });
            vertx.setPeriodic(1000, id -> {
                sendNext(producer, topic);
            });
        } else {
            LOGGER.info("XXXXXXXXXXXXXXXXXX");
            sendNext(producer, topic);
        }

    }

    private void sendNext(KafkaProducer<String, String> producer, String topic) {
        if (msgCntPredicate.negate().test(numSent.get())) {

            KafkaProducerRecord<String, String> record =
                    KafkaProducerRecord.create(topic, "message_" + numSent.get());

            producer.write(record, done -> {
                if (done.succeeded()) {
                    RecordMetadata recordMetadata = done.result();
                    LOGGER.info("Message " + record.value() + " written on topic=" + recordMetadata.getTopic() +
                            ", partition=" + recordMetadata.getPartition() +
                            ", offset=" + recordMetadata.getOffset());

                    numSent.getAndIncrement();

                    if (msgCntPredicate.test(numSent.get())) {
                        LOGGER.info("Producer sent {} messages", numSent.get());
                        resultPromise.complete(numSent.get());
                    }

                    if (msgCntPredicate.negate().test(-1)) {
                        LOGGER.info("YYYYYYYYY");
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
