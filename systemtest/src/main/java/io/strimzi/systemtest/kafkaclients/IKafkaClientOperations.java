/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients;

public interface IKafkaClientOperations<T> {

    T sendMessagesPlain(long timeoutMs) throws Exception;
    T sendMessagesTls(long timeoutMs) throws Exception;

    T receiveMessagesPlain(long timeoutMs) throws Exception;
    T receiveMessagesTls(long timeoutMs) throws Exception;
}
