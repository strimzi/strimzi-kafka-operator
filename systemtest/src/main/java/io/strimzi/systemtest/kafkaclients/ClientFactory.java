/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients;

import io.strimzi.systemtest.kafkaclients.externalclient.ExternalKafkaClient;
import io.strimzi.systemtest.kafkaclients.internalclients.KafkaClient;
import io.strimzi.systemtest.kafkaclients.internalclients.OauthKafkaClient;
import io.strimzi.systemtest.kafkaclients.internalclients.TracingKafkaClient;

public class ClientFactory {

    public static IKafkaClient getClient(String clientType) {

        if (clientType.equalsIgnoreCase(EClientType.BASIC.getClientType())) {
            return new KafkaClient();
        } else if (clientType.equalsIgnoreCase(EClientType.EXTERNAL.getClientType())) {
            return new ExternalKafkaClient();
        } else if (clientType.equalsIgnoreCase(EClientType.OAUTH.getClientType())) {
            return new OauthKafkaClient();
        } else if (clientType.equalsIgnoreCase(EClientType.TRACING.getClientType())) {
            return new TracingKafkaClient();
        }
        return null;
    }
}
