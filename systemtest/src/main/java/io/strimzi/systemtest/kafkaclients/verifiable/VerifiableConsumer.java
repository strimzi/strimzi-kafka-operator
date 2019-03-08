/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.verifiable;

import io.strimzi.systemtest.kafkaclients.AbstractClient;
import io.strimzi.systemtest.kafkaclients.ClientArgument;
import io.strimzi.systemtest.kafkaclients.ClientType;

public class VerifiableConsumer extends AbstractClient {
    public VerifiableConsumer() {
        super(ClientType.CLI_KAFKA_VERIFIABLE_CONSUMER);
    }

    @Override
    protected void fillAllowedArgs() {
        allowedArgs.add(ClientArgument.BROKER_LIST);
        allowedArgs.add(ClientArgument.TOPIC);
        allowedArgs.add(ClientArgument.GROUP_ID);
        allowedArgs.add(ClientArgument.MAX_MESSAGES);
        allowedArgs.add(ClientArgument.SESSION_TIMEOUT);
        allowedArgs.add(ClientArgument.VERBOSE);
        allowedArgs.add(ClientArgument.ENABLE_AUTOCOMMIT);
        allowedArgs.add(ClientArgument.RESET_POLICY);
        allowedArgs.add(ClientArgument.ASSIGMENT_STRATEGY);
        allowedArgs.add(ClientArgument.CONSUMER_CONFIG);
    }
}
