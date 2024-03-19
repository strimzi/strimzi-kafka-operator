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

import java.io.Serializable;
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
public class GenericKafkaListenerConfigurationBootstrap implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private List<String> alternativeNames;
    private String host;
    private Map<String, String> annotations = new HashMap<>(0);
    private Map<String, String> labels = new HashMap<>(0);
    private Integer nodePort;
    private String loadBalancerIP;
    private List<String> externalIPs;

    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Additional alternative names for the bootstrap service. " +
            "The alternative names will be added to the list of subject alternative names of the TLS certificates.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<String> getAlternativeNames() {
        return alternativeNames;
    }

    public void setAlternativeNames(List<String> alternativeNames) {
        this.alternativeNames = alternativeNames;
    }

    @Description("The bootstrap host. " +
            "This field will be used in the Ingress resource or in the Route resource to specify the desired hostname. " +
            "This field can be used only with `route` (optional) or `ingress` (required) type listeners.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Description("Annotations that will be added to the `Ingress`, `Route`, or `Service` resource. " +
            "You can use this field to configure DNS providers such as External DNS. " +
            "This field can be used only with `loadbalancer`, `nodeport`, `route`, or `ingress` type listeners.")
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

    @Description("Node port for the bootstrap service. " +
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
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
