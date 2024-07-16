/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configures listener bootstrap configuration
 */
@DescriptionFile
@JsonPropertyOrder({"alternativeNames", "host", "dnsAnnotations", "nodePort", "loadBalancerIP",
    "annotations", "labels", "externalIPs"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
    editableEnabled = false,
    builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@EqualsAndHashCode
@ToString
public class GenericKafkaListenerConfigurationBootstrap implements UnknownPropertyPreserving {
    private List<String> alternativeNames;
    private String host;
    private Map<String, String> annotations = new HashMap<>(0);
    private Map<String, String> labels = new HashMap<>(0);
    private Integer nodePort;
    private String loadBalancerIP;
    private List<String> externalIPs;
    private Map<String, Object> additionalProperties;

    @Description("Additional alternative names for the bootstrap service. " +
            "The alternative names will be added to the list of subject alternative names of the TLS certificates.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<String> getAlternativeNames() {
        return alternativeNames;
    }

    public void setAlternativeNames(List<String> alternativeNames) {
        this.alternativeNames = alternativeNames;
    }

    @Description("Specifies the hostname used for the bootstrap resource. " +
        "For `route` (optional) or `ingress` (required) listeners only. " +
        "Ensure the hostname resolves to the Ingress endpoints; no validation is performed by Strimzi.") 
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Description("Annotations added to `Ingress`, `Route`, or `Service` resources. " +
            "You can use this property to configure DNS providers such as External DNS. " +
            "For `loadbalancer`, `nodeport`, `route`, or `ingress` listeners only.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, String> getAnnotations() {
        return annotations;
    }

    public void setAnnotations(Map<String, String> annotations) {
        this.annotations = annotations;
    }

    @Description("Labels added to `Ingress`, `Route`, or `Service` resources. " +
            "For `loadbalancer`, `nodeport`, `route`, or `ingress` listeners only.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, String> getLabels() {
        return labels;
    }

    public void setLabels(Map<String, String> labels) {
        this.labels = labels;
    }

    @Description("Node port for the bootstrap service. " +
            "For `nodeport` listeners only.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getNodePort() {
        return nodePort;
    }

    public void setNodePort(Integer nodePort) {
        this.nodePort = nodePort;
    }

    @Description("The loadbalancer is requested with the IP address specified in this property. " +
            "This feature depends on whether the underlying cloud provider supports specifying the `loadBalancerIP` when a load balancer is created. " +
            "This property is ignored if the cloud provider does not support the feature. " +
            "For `loadbalancer` listeners only.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String getLoadBalancerIP() {
        return loadBalancerIP;
    }

    public void setLoadBalancerIP(String loadBalancerIP) {
        this.loadBalancerIP = loadBalancerIP;
    }

    @Description("External IPs associated to the nodeport service. " + 
            "These IPs are used by clients external to the Kubernetes cluster to access the Kafka brokers. " +
            "This property is helpful when `nodeport` without `externalIP` is not sufficient. For example on bare-metal Kubernetes clusters that do not support Loadbalancer service types. " +
            "For `nodeport` listeners only.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<String> getExternalIPs() {
        return externalIPs;
    }

    public void setExternalIPs(List<String> externalIPs) {
        this.externalIPs = externalIPs;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : Map.of();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(2);
        }
        this.additionalProperties.put(name, value);
    }
}
