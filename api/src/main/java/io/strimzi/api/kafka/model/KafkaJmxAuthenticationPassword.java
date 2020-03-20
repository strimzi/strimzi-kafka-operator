/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

/**
 * Configures the Kafka Brokers JMX port with username and password protected.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
public class KafkaJmxAuthenticationPassword extends KafkaJmxAuthentication {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_PASSWORD = "password";

    @Description("Must be `" + TYPE_PASSWORD + "`")
    @Override
    public String getType() {
        return TYPE_PASSWORD;
    }

}
