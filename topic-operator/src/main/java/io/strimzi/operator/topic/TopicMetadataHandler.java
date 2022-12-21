/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.MaxAttemptsExceededException;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * Done
 * Represents a handler for getting Kafka topic metadata, providing a helper {@link #retry} method
 * for subclasses which want to retry when they need to do that
 */
public abstract class TopicMetadataHandler implements Handler<AsyncResult<TopicMetadata>> {

    private static final Logger LOGGER = LogManager.getLogger(TopicMetadataHandler.class);

    private final BackOff backOff;

    private final Vertx vertx;
    private final Kafka kafka;
    private final TopicName topicName;

    /**
     * Constructor
     *
     * @param vertx Vert.x instance to use for retrying mechanism
     * @param kafka Kafka client for getting topic metadata
     * @param topicName topic name for which to get metadata
     * @param backOff   backoff information to use for retrying
     */
    TopicMetadataHandler(Vertx vertx, Kafka kafka, TopicName topicName, BackOff backOff) {
        this.vertx = vertx;
        this.kafka = kafka;
        this.topicName = topicName;
        this.backOff = backOff;
    }

    /**
     * Constructor
     *
     * @param vertx Vert.x instance to use for retrying mechanism
     * @param kafka Kafka client for getting topic metadata
     * @param topicName topic name for which to get metadata
     */
    TopicMetadataHandler(Vertx vertx, Kafka kafka, TopicName topicName) {
        this(vertx, kafka, topicName, new BackOff());
    }

    /**
     * Schedules this handler to execute again after a delay defined by the {@code BackOff}.
     * Calls {@link #onMaxAttemptsExceeded} if the backoff has reached its permitted number of retries.
     */
    protected void retry(Reconciliation reconciliation) {

        long delay;
        try {
            delay = backOff.delayMs();
            LOGGER.debug("Backing off for {}ms on getting metadata for {}", delay, topicName);
        } catch (MaxAttemptsExceededException e) {
            LOGGER.info("Max attempts reached on getting metadata for {} after {}ms, giving up for now", topicName, backOff.totalDelayMs());
            this.onMaxAttemptsExceeded(e);
            return;
        }

        if (delay < 1) {
            // vertx won't tolerate a zero delay
            vertx.runOnContext(timerId -> kafka.topicMetadata(reconciliation, topicName).onComplete(this));
        } else {
            vertx.setTimer(TimeUnit.MILLISECONDS.convert(delay, TimeUnit.MILLISECONDS),
                timerId -> kafka.topicMetadata(reconciliation, topicName).onComplete(this));
        }
    }

    /**
     * Called when the max attempts are exceeded during retry
     *
     * @param e the max attempts exceeded exception instance
     */
    public abstract void onMaxAttemptsExceeded(MaxAttemptsExceededException e);
}
