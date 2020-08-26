/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyPeer;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * Configures the external listener which exposes Kafka outside of Kubernetes using Ingress
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "authentication", "overrides"})
@EqualsAndHashCode
public class KafkaListenerExternalIngress extends KafkaListenerExternal {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_INGRESS = "ingress";

    private KafkaListenerAuthentication auth;
    private List<NetworkPolicyPeer> networkPolicyPeers;
    private IngressListenerConfiguration configuration;
    private String ingressClass;

    @Description("Must be `" + TYPE_INGRESS + "`")
    @Override
    public String getType() {
        return TYPE_INGRESS;
    }

    @Override
    @Description("Authentication configuration for Kafka brokers")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("authentication")
    public KafkaListenerAuthentication getAuth() {
        return auth;
    }

    @Override
    public void setAuth(KafkaListenerAuthentication auth) {
        this.auth = auth;
    }

    @Override
    @Description("List of peers which should be able to connect to this listener. " +
            "Peers in this list are combined using a logical OR operation. " +
            "If this field is empty or missing, all connections will be allowed for this listener. " +
            "If this field is present and contains at least one item, the listener only allows the traffic which matches at least one item in this list.")
    @KubeLink(group = "networking.k8s.io", version = "v1", kind = "networkpolicypeer")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<NetworkPolicyPeer> getNetworkPolicyPeers() {
        return networkPolicyPeers;
    }

    @Override
    public void setNetworkPolicyPeers(List<NetworkPolicyPeer> networkPolicyPeers) {
        this.networkPolicyPeers = networkPolicyPeers;
    }

    @Description("External listener configuration")
    public IngressListenerConfiguration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(IngressListenerConfiguration configuration) {
        this.configuration = configuration;
    }

    @Description("Configures the `Ingress` class that defines which `Ingress` controller will be used. " +
            "If not set, the `Ingress` class is set to `nginx`.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("class")
    public String getIngressClass() {
        return ingressClass;
    }

    public void setIngressClass(String ingressClass) {
        this.ingressClass = ingressClass;
    }
}
