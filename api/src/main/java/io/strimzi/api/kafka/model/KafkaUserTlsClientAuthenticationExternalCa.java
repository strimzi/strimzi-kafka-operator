/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
public class KafkaUserTlsClientAuthenticationExternalCa extends KafkaUserAuthentication {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_TLS = "tls-external-ca";
    private String name;

    @Description("Must be `" + TYPE_TLS + "`")
    @Override
    public String getType() {
        return TYPE_TLS;
    }

    @Description("Authentication CN name.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
