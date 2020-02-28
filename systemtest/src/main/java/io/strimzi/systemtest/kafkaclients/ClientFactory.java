/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients;

import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.kafkaclients.externalClients.KafkaClient;
import io.strimzi.systemtest.kafkaclients.externalClients.OauthKafkaClient;
import io.strimzi.systemtest.kafkaclients.externalClients.TracingKafkaClient;

public class ClientFactory {

    public static IKafkaClient<?> getClient(EClientType clientType) {

        switch (clientType) {
            case BASIC:
                return new KafkaClient();
            case INTERNAL:
                return new InternalKafkaClient();
            case OAUTH:
                return new OauthKafkaClient();
            case TRACING:
                return new TracingKafkaClient();
        }
        return null;
    }
}
