/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.template;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Representation of a template for Strimzi external services.
 * It contains additional values applicable to load balancer or node port type services.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"metadata", "externalTrafficPolicy", "loadBalancerSourceRanges"})
@DescriptionFile
@EqualsAndHashCode
public class ExternalServiceTemplate implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private MetadataTemplate metadata;
    private ExternalTrafficPolicy externalTrafficPolicy;
    private List<String> loadBalancerSourceRanges = new ArrayList<>(0);
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Metadata applied to the resource.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public MetadataTemplate getMetadata() {
        return metadata;
    }

    public void setMetadata(MetadataTemplate metadata) {
        this.metadata = metadata;
    }

    @Description("Specifies whether the service routes external traffic to node-local or cluster-wide endpoints. " +
            "`Cluster` may cause a second hop to another node and obscures the client source IP. " +
            "`Local` avoids a second hop for LoadBalancer and Nodeport type services and preserves the client source IP (when supported by the infrastructure). " +
            "If unspecified, Kubernetes will use `Cluster` as the default.")
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
            "For more information, see https://kubernetes.io/docs/tasks/access-application-cluster/configure-cloud-provider-firewall/")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<String> getLoadBalancerSourceRanges() {
        return loadBalancerSourceRanges;
    }

    public void setLoadBalancerSourceRanges(List<String> loadBalancerSourceRanges) {
        this.loadBalancerSourceRanges = loadBalancerSourceRanges;
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
