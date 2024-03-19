/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Configures listener per-broker configuration
 */
@DescriptionFile
@JsonPropertyOrder({"broker", "advertisedHost", "advertisedPort", "host", "dnsAnnotations", "nodePort",
    "loadBalancerIP", "annotations", "labels", "externalIPs"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
    editableEnabled = false,
    builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@EqualsAndHashCode
@ToString
public class GenericKafkaListenerConfigurationBroker implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private Integer broker;
    private String advertisedHost;
    private Integer advertisedPort;
    private String host;
    private Map<String, String> annotations = new HashMap<>(0);
    private Map<String, String> labels = new HashMap<>(0);
    private Integer nodePort;
    private String loadBalancerIP;
    private List<String> externalIPs;

    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("ID of the kafka broker (broker identifier). " +
            "Broker IDs start from 0 and correspond to the number of broker replicas.")
    @JsonProperty(required = true)
    public Integer getBroker() {
        return broker;
    }

    public void setBroker(Integer broker) {
        this.broker = broker;
    }

    @Description("The host name used in the brokers' `advertised.listeners`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String getAdvertisedHost() {
        return advertisedHost;
    }

    public void setAdvertisedHost(String advertisedHost) {
        this.advertisedHost = advertisedHost;
    }

    @Description("The port number used in the brokers' `advertised.listeners`.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getAdvertisedPort() {
        return advertisedPort;
    }

    public void setAdvertisedPort(Integer advertisedPort) {
        this.advertisedPort = advertisedPort;
    }

    @Description("The broker host. " +
            "This field will be used in the Ingress resource or in the Route resource to specify the desired hostname. " +
            "This field can be used only with `route` (optional) or `ingress` (required) type listeners.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Description("Annotations that will be added to the `Ingress` or `Service` resource. " +
            "You can use this field to configure DNS providers such as External DNS. " +
            "This field can be used only with `loadbalancer`, `nodeport`, or `ingress` type listeners.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, String> getAnnotations() {
        return annotations;
    }

    public void setAnnotations(Map<String, String> annotations) {
        this.annotations = annotations;
    }

    @Description("Labels that will be added to the `Ingress`, `Route`, or `Service` resource. " +
            "This field can be used only with `loadbalancer`, `nodeport`, `route`, or `ingress` type listeners.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, String> getLabels() {
        return labels;
    }

    public void setLabels(Map<String, String> labels) {
        this.labels = labels;
    }

    @Description("Node port for the per-broker service. " +
            "This field can be used only with `nodeport` type listener.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getNodePort() {
        return nodePort;
    }

    public void setNodePort(Integer nodePort) {
        this.nodePort = nodePort;
    }

    @Description("The loadbalancer is requested with the IP address specified in this field. " +
            "This feature depends on whether the underlying cloud provider supports specifying the `loadBalancerIP` when a load balancer is created. " +
            "This field is ignored if the cloud provider does not support the feature." +
            "This field can be used only with `loadbalancer` type listener.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String getLoadBalancerIP() {
        return loadBalancerIP;
    }

    public void setLoadBalancerIP(String loadBalancerIP) {
        this.loadBalancerIP = loadBalancerIP;
    }

    @Description("External IPs associated to the nodeport service. " + 
            "These IPs are used by clients external to the Kubernetes cluster to access the Kafka brokers. " +
            "This field is helpful when `nodeport` without `externalIP` is not sufficient. For example on bare-metal Kubernetes clusters that do not support Loadbalancer service types. " +
            "This field can only be used with `nodeport` type listener.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<String> getExternalIPs() {
        return externalIPs;
    }

    public void setExternalIPs(List<String> externalIPs) {
        this.externalIPs = externalIPs;
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
