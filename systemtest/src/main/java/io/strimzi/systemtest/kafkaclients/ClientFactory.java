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

    public static IKafkaClient getClient(String clientType) {

        if (clientType.equalsIgnoreCase(EClientType.BASIC.getClientType())) {
            return new KafkaClient();
        } else if (clientType.equalsIgnoreCase(EClientType.EXTERNAL.getClientType())) {
            return new InternalKafkaClient();
        } else if (clientType.equalsIgnoreCase(EClientType.OAUTH.getClientType())) {
            return new OauthKafkaClient();
        } else if (clientType.equalsIgnoreCase(EClientType.TRACING.getClientType())) {
            return new TracingKafkaClient();
        }
        return null;
    }
}
