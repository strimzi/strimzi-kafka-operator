/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.clients.lib;

import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

        if (msgCntPredicate.test(-1)) {
            vertx.eventBus().consumer(clientName, msg -> {
                if (msg.body().equals("stop")) {
                    LOGGER.debug("Received stop command! Produced messages: {}", numSent.get());
                    resultPromise.complete(numSent.get());
                }
            });
            vertx.setPeriodic(1000, id -> sendNext(producer, topic));
        } else {
            sendNext(producer, topic);
        }

    }

    private void sendNext(KafkaProducer<String, String> producer, String topic) {
        if (msgCntPredicate.negate().test(numSent.get())) {

            KafkaProducerRecord<String, String> record =
                    KafkaProducerRecord.create(topic, String.valueOf(numSent.get()));

            producer.send(record, done -> {
                if (done.succeeded()) {
                    RecordMetadata recordMetadata = done.result();
                    LOGGER.debug("Message " + record.value() + " written on topic=" + recordMetadata.getTopic() +
                            ", partition=" + recordMetadata.getPartition() +
                            ", offset=" + recordMetadata.getOffset());

                    numSent.getAndIncrement();

                    if (msgCntPredicate.test(numSent.get())) {
                        LOGGER.info("Producer produced {} messages", numSent.get());
                        resultPromise.complete(numSent.get());
                    }

                    if (msgCntPredicate.negate().test(-1)) {
                        sendNext(producer, topic);
                    }

                } else {
                    LOGGER.warn("Producer cannot connect to topic {}: {}", topic, done.cause().toString());
                    sendNext(producer, topic);
                }
            });

        }
    }
}
