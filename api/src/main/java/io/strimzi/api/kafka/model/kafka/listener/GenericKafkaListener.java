/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeer;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.strimzi.crdgenerator.annotations.Pattern;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configures the external listener which exposes Kafka outside of Kubernetes / OpenShift
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonPropertyOrder({"name", "port", "type", "tls", "authentication", "configuration", "networkPolicyPeers"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
@ToString
public class GenericKafkaListener implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    // maximal port name length is 15. The prefix of generic port name is 'tcp-'
    public final static String LISTENER_NAME_REGEX = "^[a-z0-9]{1,11}$";

    private String name;
    private int port;
    private KafkaListenerType type;
    private boolean tls = false;
    private KafkaListenerAuthentication auth;
    private GenericKafkaListenerConfiguration configuration;
    private List<NetworkPolicyPeer> networkPolicyPeers;
    private Map<String, Object> additionalProperties;

    @Description("Name of the listener. " +
            "The name will be used to identify the listener and the related Kubernetes objects. " +
            "The name has to be unique within given a Kafka cluster. " +
            "The name can consist of lowercase characters and numbers and be up to 11 characters long.")
    @JsonProperty(required = true)
    @Pattern(LISTENER_NAME_REGEX)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Description("Port number used by the listener inside Kafka. " +
            "The port number has to be unique within a given Kafka cluster. " +
            "Allowed port numbers are 9092 and higher with the exception of ports 9404 and 9999, which are already used for Prometheus and JMX. " +
            "Depending on the listener type, the port number might not be the same as the port number that connects Kafka clients.")
    @JsonProperty(required = true)
    @Minimum(9092)
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Description("Type of the listener. " +
            "The supported types are as follows: \n\n" +
            "* `internal` type exposes Kafka internally only within the Kubernetes cluster.\n" +
            "* `route` type uses OpenShift Routes to expose Kafka.\n" +
            "* `loadbalancer` type uses LoadBalancer type services to expose Kafka.\n" +
            "* `nodeport` type uses NodePort type services to expose Kafka.\n" +
            "* `ingress` type uses Kubernetes Nginx Ingress to expose Kafka with TLS passthrough.\n" +
            "* `cluster-ip` type uses a per-broker `ClusterIP` service.\n")
    @JsonProperty(required = true)
    public KafkaListenerType getType() {
        return type;
    }

    public void setType(KafkaListenerType type) {
        this.type = type;
    }

    @Description("Authentication configuration for this listener")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("authentication")
    public KafkaListenerAuthentication getAuth() {
        return auth;
    }

    public void setAuth(KafkaListenerAuthentication auth) {
        this.auth = auth;
    }

    @Description("Enables TLS encryption on the listener. " +
            "This is a required property.")
    @JsonProperty(required = true)
    public boolean isTls() {
        return tls;
    }

    public void setTls(boolean tls) {
        this.tls = tls;
    }

    @Description("Additional listener configuration")
    public GenericKafkaListenerConfiguration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(GenericKafkaListenerConfiguration configuration) {
        this.configuration = configuration;
    }

    @Description("List of peers which should be able to connect to this listener. " +
            "Peers in this list are combined using a logical OR operation. " +
            "If this field is empty or missing, all connections will be allowed for this listener. " +
            "If this field is present and contains at least one item, the listener only allows the traffic which matches at least one item in this list.")
    @KubeLink(group = "networking.k8s.io", version = "v1", kind = "networkpolicypeer")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<NetworkPolicyPeer> getNetworkPolicyPeers() {
        return networkPolicyPeers;
    }

    public void setNetworkPolicyPeers(List<NetworkPolicyPeer> networkPolicyPeers) {
        this.networkPolicyPeers = networkPolicyPeers;
    }

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
