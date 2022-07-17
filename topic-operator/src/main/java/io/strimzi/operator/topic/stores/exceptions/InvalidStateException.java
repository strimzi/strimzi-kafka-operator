/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.stores.exceptions;

/**
 * Throw this when TopicStore's state is invalid.
 * e.g. in the case of KafkaStreamsTopicStore we throw this when
 * waiting on a async result takes too long -- see Config#STALE_RESULT_TIMEOUT_MS
 */
public class InvalidStateException extends Exception {

}
