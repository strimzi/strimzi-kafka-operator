/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.client.CustomResource;

@JsonPropertyOrder({"apiVersion", "kind", "metadata", "spec", "status"})
public class KafkaConnector extends CustomResource {
    private KafkaConnectorSpec spec;

    public KafkaConnectorSpec getSpec() {
        return spec;
    }
}

