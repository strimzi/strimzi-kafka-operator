/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.rebalance;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.MinimumItems;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configures the broker and Volume ID's for the remove-disks endpoint for Cruise Control
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonPropertyOrder({"brokerId", "volumeIds"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
@ToString
public class BrokerAndVolumeIds implements UnknownPropertyPreserving {

    private Integer brokerId;
    private List<Integer> volumeIds;
    private Map<String, Object> additionalProperties;

    @Description("ID of the broker that contains the disk from which you want to move the partition replicas.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("brokerId")
    public Integer getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(Integer brokerId) {
        this.brokerId = brokerId;
    }

    @Description("IDs of the disks from which the partition replicas need to be moved.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("volumeIds")
    @MinimumItems(1)
    public List<Integer> getVolumeIds() {
        return volumeIds;
    }

    public void setVolumeIds(List<Integer> volumeIds) {
        this.volumeIds = volumeIds;
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
