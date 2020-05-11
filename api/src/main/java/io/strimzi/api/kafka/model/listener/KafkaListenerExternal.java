/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyPeer;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configures the external listener which exposes Kafka outside of Kubernetes / OpenShift
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(name = KafkaListenerExternalRoute.TYPE_ROUTE, value = KafkaListenerExternalRoute.class),
        @JsonSubTypes.Type(name = KafkaListenerExternalLoadBalancer.TYPE_LOADBALANCER, value = KafkaListenerExternalLoadBalancer.class),
        @JsonSubTypes.Type(name = KafkaListenerExternalNodePort.TYPE_NODEPORT, value = KafkaListenerExternalNodePort.class),
        @JsonSubTypes.Type(name = KafkaListenerExternalIngress.TYPE_INGRESS, value = KafkaListenerExternalIngress.class),
})
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
public abstract class KafkaListenerExternal implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    private Map<String, Object> additionalProperties;

    @Description("Type of the external listener. " +
            "Currently the supported types are `route`, `loadbalancer`, and `nodeport`. \n\n" +
            "* `route` type uses OpenShift Routes to expose Kafka." +
            "* `loadbalancer` type uses LoadBalancer type services to expose Kafka." +
            "* `nodeport` type uses NodePort type services to expose Kafka.")
    public abstract String getType();

    @Description("Authentication configuration for Kafka brokers")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("authentication")
    public abstract KafkaListenerAuthentication getAuth();

    public abstract void setAuth(KafkaListenerAuthentication auth);

    @Description("List of peers which should be able to connect to this listener. " +
            "Peers in this list are combined using a logical OR operation. " +
            "If this field is empty or missing, all connections will be allowed for this listener. " +
            "If this field is present and contains at least one item, the listener only allows the traffic which matches at least one item in this list.")
    @KubeLink(group = "networking.k8s.io", version = "v1", kind = "networkpolicypeer")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public abstract List<NetworkPolicyPeer> getNetworkPolicyPeers();

    public abstract void setNetworkPolicyPeers(List<NetworkPolicyPeer> networkPolicyPeers);

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(1);
        }
        this.additionalProperties.put(name, value);
    }
}
