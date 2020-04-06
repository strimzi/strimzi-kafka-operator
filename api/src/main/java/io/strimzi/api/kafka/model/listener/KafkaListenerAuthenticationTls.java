/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import io.strimzi.api.kafka.model.Constants;
import io.strimzi.crdgenerator.annotations.Description;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

/**
 * Configures a listener to use mutual TLS authentication.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
public class KafkaListenerAuthenticationTls extends KafkaListenerAuthentication {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_TLS = "tls";

    @Description("Must be `" + TYPE_TLS + "`")
    @Override
    public String getType() {
        return TYPE_TLS;
    }
}
