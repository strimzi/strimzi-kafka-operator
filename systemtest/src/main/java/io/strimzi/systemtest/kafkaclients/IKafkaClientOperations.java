/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public interface IKafkaClientOperations<T> {

    T sendMessagesPlain(long timeoutMs) throws InterruptedException, ExecutionException, TimeoutException, Exception;
    T sendMessagesTls(long timeoutMs) throws InterruptedException, ExecutionException, TimeoutException, Exception;

    T receiveMessagesPlain(long timeoutMs) throws InterruptedException, ExecutionException, TimeoutException, Exception;
    T receiveMessagesTls(long timeoutMs) throws InterruptedException, ExecutionException, TimeoutException, Exception;
}
