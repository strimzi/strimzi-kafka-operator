/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.template;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.PresentInVersions;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Representation of a template for Strimzi internal services.
 * It contains additional values applicable to internal services..
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"metadata", "ipFamilyPolicy", "ipFamilies"})
@EqualsAndHashCode
@ToString
public class InternalServiceTemplate implements HasMetadataTemplate, Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private MetadataTemplate metadata;
    private IpFamilyPolicy ipFamilyPolicy;
    private List<IpFamily> ipFamilies;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Metadata applied to the resource.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public MetadataTemplate getMetadata() {
        return metadata;
    }

    public void setMetadata(MetadataTemplate metadata) {
        this.metadata = metadata;
    }

    @Description("Specifies the IP Family Policy used by the service. " +
            "Available options are `SingleStack`, `PreferDualStack` and `RequireDualStack`. " +
            "`SingleStack` is for a single IP family. " +
            "`PreferDualStack` is for two IP families on dual-stack configured clusters or a single IP family on single-stack clusters. " +
            "`RequireDualStack` fails unless there are two IP families on dual-stack configured clusters. " +
            "If unspecified, Kubernetes will choose the default value based on the service type.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @PresentInVersions("v1beta2+")
    public IpFamilyPolicy getIpFamilyPolicy() {
        return ipFamilyPolicy;
    }

    public void setIpFamilyPolicy(IpFamilyPolicy ipFamilyPolicy) {
        this.ipFamilyPolicy = ipFamilyPolicy;
    }

    @Description("Specifies the IP Families used by the service. " +
            "Available options are `IPv4` and `IPv6`. " +
            "If unspecified, Kubernetes will choose the default value based on the `ipFamilyPolicy` setting.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @PresentInVersions("v1beta2+")
    public List<IpFamily> getIpFamilies() {
        return ipFamilies;
    }

    public void setIpFamilies(List<IpFamily> ipFamilies) {
        this.ipFamilies = ipFamilies;
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
