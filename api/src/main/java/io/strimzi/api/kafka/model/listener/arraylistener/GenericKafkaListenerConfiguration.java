/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener.arraylistener;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.CertAndKeySecretSource;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.api.kafka.model.listener.NodeAddressType;
import io.strimzi.api.kafka.model.template.ExternalTrafficPolicy;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Configures Kafka listeners
 */
@DescriptionFile
@JsonPropertyOrder({"brokerCertChainAndKey", "ingressClass", "preferredAddressType", "externalTrafficPolicy", "loadBalancerSourceRanges", "bootstrap", "brokers"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Buildable(
    editableEnabled = false,
    builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@EqualsAndHashCode
public class GenericKafkaListenerConfiguration implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private CertAndKeySecretSource brokerCertChainAndKey;
    private String ingressClass;
    private NodeAddressType preferredNodePortAddressType;
    private ExternalTrafficPolicy externalTrafficPolicy;
    private List<String> loadBalancerSourceRanges;
    private List<String> finalizers;
    private Boolean useServiceDnsDomain;
    private GenericKafkaListenerConfigurationBootstrap bootstrap;
    private List<GenericKafkaListenerConfigurationBroker> brokers;
    private Integer maxConnections;
    private Integer maxConnectionCreationRate;

    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Reference to the `Secret` which holds the certificate and private key pair which will be used for this listener. " +
            "The certificate can optionally contain the whole chain. " +
            "This field can be used only with listeners with enabled TLS encryption.")
    public CertAndKeySecretSource getBrokerCertChainAndKey() {
        return brokerCertChainAndKey;
    }

    public void setBrokerCertChainAndKey(CertAndKeySecretSource brokerCertChainAndKey) {
        this.brokerCertChainAndKey = brokerCertChainAndKey;
    }

    @Description("Configures the `Ingress` class that defines which `Ingress` controller will be used. " +
            "This field can be used only with `ingress` type listener. " +
            "If not specified, the default Ingress controller will be used.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("class")
    public String getIngressClass() {
        return ingressClass;
    }

    public void setIngressClass(String ingressClass) {
        this.ingressClass = ingressClass;
    }

    @Description("Defines which address type should be used as the node address. " +
            "Available types are: `ExternalDNS`, `ExternalIP`, `InternalDNS`, `InternalIP` and `Hostname`. " +
            "By default, the addresses will be used in the following order (the first one found will be used):\n" +
            "* `ExternalDNS`\n" +
            "* `ExternalIP`\n" +
            "* `InternalDNS`\n" +
            "* `InternalIP`\n" +
            "* `Hostname`\n" +
            "\n" +
            "This field can be used to select the address type which will be used as the preferred type and checked first. " +
            "In case no address will be found for this address type, the other types will be used in the default order." +
            "This field can be used only with `nodeport` type listener.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public NodeAddressType getPreferredNodePortAddressType() {
        return preferredNodePortAddressType;
    }

    public void setPreferredNodePortAddressType(NodeAddressType preferredNodePortAddressType) {
        this.preferredNodePortAddressType = preferredNodePortAddressType;
    }

    @Description("Specifies whether the service routes external traffic to node-local or cluster-wide endpoints. " +
            "`Cluster` may cause a second hop to another node and obscures the client source IP. " +
            "`Local` avoids a second hop for LoadBalancer and Nodeport type services and preserves the client source IP (when supported by the infrastructure). " +
            "If unspecified, Kubernetes will use `Cluster` as the default." +
            "This field can be used only with `loadbalancer` or `nodeport` type listener.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public ExternalTrafficPolicy getExternalTrafficPolicy() {
        return externalTrafficPolicy;
    }

    public void setExternalTrafficPolicy(ExternalTrafficPolicy externalTrafficPolicy) {
        this.externalTrafficPolicy = externalTrafficPolicy;
    }

    @Description("A list of CIDR ranges (for example `10.0.0.0/8` or `130.211.204.1/32`) from which clients can connect to load balancer type listeners. " +
            "If supported by the platform, traffic through the loadbalancer is restricted to the specified CIDR ranges. " +
            "This field is applicable only for loadbalancer type services and is ignored if the cloud provider does not support the feature. " +
            "For more information, see https://v1-17.docs.kubernetes.io/docs/tasks/access-application-cluster/configure-cloud-provider-firewall/. " +
            "This field can be used only with `loadbalancer` type listener.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<String> getLoadBalancerSourceRanges() {
        return loadBalancerSourceRanges;
    }

    public void setLoadBalancerSourceRanges(List<String> loadBalancerSourceRanges) {
        this.loadBalancerSourceRanges = loadBalancerSourceRanges;
    }

    @Description("A list of finalizers which will be configured for the `LoadBalancer` type Services created for this listener. " +
            "If supported by the platform, the finalizer `service.kubernetes.io/load-balancer-cleanup` to make sure that the external load balancer is deleted together with the service." +
            "For more information, see https://kubernetes.io/docs/tasks/access-application-cluster/create-external-load-balancer/#garbage-collecting-load-balancers. " +
            "This field can be used only with `loadbalancer` type listeners.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<String> getFinalizers() {
        return finalizers;
    }

    public void setFinalizers(List<String> finalizers) {
        this.finalizers = finalizers;
    }

    @Description("Configures whether the Kubernetes service DNS domain should be used or not. " +
            "If set to `true`, the generated addresses will contain the service DNS domain suffix " +
            "(by default `.cluster.local`, can be configured using environment variable `KUBERNETES_SERVICE_DNS_DOMAIN`). " +
            "Defaults to `false`." +
            "This field can be used only with `internal` type listener.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Boolean getUseServiceDnsDomain() {
        return useServiceDnsDomain;
    }

    public void setUseServiceDnsDomain(Boolean useServiceDnsDomain) {
        this.useServiceDnsDomain = useServiceDnsDomain;
    }

    @Description("Bootstrap configuration.")
    public GenericKafkaListenerConfigurationBootstrap getBootstrap() {
        return bootstrap;
    }

    public void setBootstrap(GenericKafkaListenerConfigurationBootstrap bootstrap) {
        this.bootstrap = bootstrap;
    }

    @Description("Per-broker configurations.")
    public List<GenericKafkaListenerConfigurationBroker> getBrokers() {
        return brokers;
    }

    public void setBrokers(List<GenericKafkaListenerConfigurationBroker> brokers) {
        this.brokers = brokers;
    }

    @Description("The maximum number of connections we allow for this listener in the broker at any time. " +
            "New connections are blocked if the limit is reached.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(Integer maxConnections) {
        this.maxConnections = maxConnections;
    }

    @Description("The maximum connection creation rate we allow in this listener at any time. " +
            "New connections will be throttled if the limit is reached." +
            "Supported only on Kafka 2.7.0 and newer.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getMaxConnectionCreationRate() {
        return maxConnectionCreationRate;
    }

    public void setMaxConnectionCreationRate(Integer maxConnectionCreationRate) {
        this.maxConnectionCreationRate = maxConnectionCreationRate;
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
