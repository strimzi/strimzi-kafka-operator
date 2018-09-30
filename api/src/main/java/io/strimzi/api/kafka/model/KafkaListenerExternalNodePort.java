/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.crdgenerator.annotations.Description;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.sundr.builder.annotations.Buildable;

/**
 * Configures the external listener which exposes Kafka outside of Kubernetes using NodePorts
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class KafkaListenerExternalNodePort extends KafkaListenerExternal {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_NODEPORT = "nodeport";

    private KafkaListenerAuthentication auth;
    private boolean tls = true;

    @Description("Must be `" + TYPE_NODEPORT + "`")
    @Override
    public String getType() {
        return TYPE_NODEPORT;
    }

    @Description("Authentication configuration for Kafka brokers")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("authentication")
    public KafkaListenerAuthentication getAuth() {
        return auth;
    }

    public void setAuth(KafkaListenerAuthentication auth) {
        this.auth = auth;
    }

    @Description("Enables TLS encryption on the listener. " +
            "By default set to `true` for enabled TLS encryption.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public boolean isTls() {
        return tls;
    }

    public void setTls(boolean tls) {
        this.tls = tls;
    }
}
