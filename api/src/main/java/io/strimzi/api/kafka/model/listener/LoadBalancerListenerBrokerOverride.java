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

import java.util.HashMap;
import java.util.Map;

/**
 * Configures external broker service and advertised addresses for LoadBalancer listeners
 */
@JsonPropertyOrder({"broker", "advertisedHost", "advertisedPort"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
    editableEnabled = false,
    builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@EqualsAndHashCode(callSuper = true)
public class LoadBalancerListenerBrokerOverride extends ExternalListenerBrokerOverride {
    private static final long serialVersionUID = 1L;

    private Map<String, String> dnsAnnotations = new HashMap<>(0);
    private String loadBalancerIP;

    @Description("Annotations that will be added to the `Service` resources for individual brokers. " +
            "You can use this field to configure DNS providers such as External DNS.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, String> getDnsAnnotations() {
        return dnsAnnotations;
    }

    public void setDnsAnnotations(Map<String, String> dnsAnnotations) {
        this.dnsAnnotations = dnsAnnotations;
    }

    @Description("The loadbalancer is requested with the IP address specified in this field. " +
            "This feature depends on whether the underlying cloud provider supports specifying the `loadBalancerIP` when a load balancer is created. " +
            "This field is ignored if the cloud provider does not support the feature.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String getLoadBalancerIP() {
        return loadBalancerIP;
    }

    public void setLoadBalancerIP(String loadBalancerIP) {
        this.loadBalancerIP = loadBalancerIP;
    }
}
