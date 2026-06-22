/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.template;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Representation of a pod template for Strimzi resources.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({"metadata", "imagePullSecrets", "securityContext", "terminationGracePeriodSeconds", "affinity",
    "tolerations", "topologySpreadConstraints", "priorityClassName", "schedulerName", "hostAliases", "dnsPolicy", "dnsConfig", 
    "enableServiceLinks", "tmpDirSizeLimit", "volumes", "templatedVolumes", "hostUsers"})
@EqualsAndHashCode(callSuper = true)
@ToString
@DescriptionFile
public class StatefulPodTemplate extends PodTemplate {
    private List<AdditionalTemplatedVolume> templatedVolumes;
    private Map<String, Object> additionalProperties;

    @Description("Additional volumes that can be mounted to the pod. " +
            "These volumes can use templates to mount different volumes into individual Pods.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<AdditionalTemplatedVolume> getTemplatedVolumes() {
        return templatedVolumes;
    }

    public void setTemplatedVolumes(List<AdditionalTemplatedVolume> templatedVolumes) {
        this.templatedVolumes = templatedVolumes;
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
