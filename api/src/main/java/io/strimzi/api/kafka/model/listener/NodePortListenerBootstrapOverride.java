/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;


/**
 * Configures external bootstrap service for NodePort listeners
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
    editableEnabled = false,
    builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@EqualsAndHashCode(callSuper = true)
public class NodePortListenerBootstrapOverride extends ExternalListenerBootstrapOverride {
    private static final long serialVersionUID = 1L;

    private Integer nodePort;
    private Map<String, String> dnsAnnotations = new HashMap<>(0);
    private Map<String, Object> additionalProperties;

    @Description("Node port for the bootstrap service")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getNodePort() {
        return nodePort;
    }

    public void setNodePort(Integer nodePort) {
        this.nodePort = nodePort;
    }

    @Description("Annotations that will be added to the `Service` resource. " +
            "You can use this field to configure DNS providers such as External DNS.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, String> getDnsAnnotations() {
        return dnsAnnotations;
    }

    public void setDnsAnnotations(Map<String, String> dnsAnnotations) {
        this.dnsAnnotations = dnsAnnotations;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : emptyMap();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(1);
        }
        this.additionalProperties.put(name, value);
    }
}
