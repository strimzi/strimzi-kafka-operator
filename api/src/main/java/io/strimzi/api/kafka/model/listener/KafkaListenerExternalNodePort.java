/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyPeer;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * Configures the external listener which exposes Kafka outside of Kubernetes using NodePorts
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "authentication", "overrides", "configuration"})
@EqualsAndHashCode
public class KafkaListenerExternalNodePort extends KafkaListenerExternal {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_NODEPORT = "nodeport";

    private KafkaListenerAuthentication auth;
    private boolean tls = true;
    private List<NetworkPolicyPeer> networkPolicyPeers;
    private NodePortListenerOverride overrides;
    private NodePortListenerConfiguration configuration;

    @Description("Must be `" + TYPE_NODEPORT + "`")
    @Override
    public String getType() {
        return TYPE_NODEPORT;
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

    @Description("Enables TLS encryption on the listener. " +
            "By default set to `true` for enabled TLS encryption.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public boolean isTls() {
        return tls;
    }

    public void setTls(boolean tls) {
        this.tls = tls;
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
    public NodePortListenerConfiguration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(NodePortListenerConfiguration configuration) {
        this.configuration = configuration;
    }

    @Description("Overrides for external bootstrap and broker services and externally advertised addresses")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public NodePortListenerOverride getOverrides() {
        return overrides;
    }

    public void setOverrides(NodePortListenerOverride overrides) {
        this.overrides = overrides;
    }
}
