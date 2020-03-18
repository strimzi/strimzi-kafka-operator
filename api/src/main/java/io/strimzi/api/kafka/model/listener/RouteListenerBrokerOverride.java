/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

/**
 * Configures external broker service and advertised addresses for Route listeners
 */
@JsonPropertyOrder({"broker", "advertisedHost", "advertisedPort", "host"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
    editableEnabled = false,
    builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@EqualsAndHashCode(callSuper = true)
public class RouteListenerBrokerOverride extends ExternalListenerBrokerOverride {
    private static final long serialVersionUID = 1L;

    private String host;

    @Description("Host for the broker route. " +
            "This field will be used in the `spec.host` field of the OpenShift Route.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }
}
