/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.verifiable;

import io.strimzi.systemtest.kafkaclients.AbstractClient;
import io.strimzi.systemtest.kafkaclients.ClientArgument;
import io.strimzi.systemtest.kafkaclients.ClientType;

public class VerifiableProducer extends AbstractClient {
    public VerifiableProducer() {
        super(ClientType.CLI_KAFKA_VERIFIABLE_PRODUCER);
    }

    @Override
    protected void fillAllowedArgs() {
        allowedArgs.add(ClientArgument.TOPIC);
        allowedArgs.add(ClientArgument.BROKER_LIST);
        allowedArgs.add(ClientArgument.MAX_MESSAGES);
        allowedArgs.add(ClientArgument.THROUGHPUT);
        allowedArgs.add(ClientArgument.ACKS);
        allowedArgs.add(ClientArgument.PRODUCER_CONFIG);
        allowedArgs.add(ClientArgument.MESSAGE_CREATE_TIME);
        allowedArgs.add(ClientArgument.VALUE_PREFIX);
        allowedArgs.add(ClientArgument.REPEATING_KEYS);
    }
}
