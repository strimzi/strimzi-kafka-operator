/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;
import java.util.Map;

/**
 * Representation for persistent claim-based storage.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonPropertyOrder({"id", "type", "size", "class", "selector", "deleteClaim", "overrides"})
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class PersistentClaimStorage extends SingleVolumeStorage {

    private static final long serialVersionUID = 1L;

    private String size;
    private String storageClass;
    private Map<String, String> selector;
    private boolean deleteClaim;
    private List<PersistentClaimStorageOverride> overrides;

    @Description("Must be `" + TYPE_PERSISTENT_CLAIM + "`")
    @Override
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getType() {
        return TYPE_PERSISTENT_CLAIM;
    }

    @Override
    @Description("Storage identification number. It is mandatory only for storage volumes defined in a storage of type 'jbod'")
    @Minimum(0)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getId() {
        return super.getId();
    }

    @Override
    public void setId(Integer id) {
        super.setId(id);
    }

    @Description("When `type=persistent-claim`, defines the size of the persistent volume claim, such as 100Gi. " +
            "Mandatory when `type=persistent-claim`.")
    public String getSize() {
        return size;
    }

    public void setSize(String size) {
        this.size = size;
    }

    @JsonProperty("class")
    @Description("The storage class to use for dynamic volume allocation.")
    public String getStorageClass() {
        return storageClass;
    }

    public void setStorageClass(String storageClass) {
        this.storageClass = storageClass;
    }

    @Description("Specifies a specific persistent volume to use. " +
            "It contains key:value pairs representing labels for selecting such a volume.")
    public Map<String, String> getSelector() {
        return selector;
    }

    public void setSelector(Map<String, String> selector) {
        this.selector = selector;
    }

    @Description("Specifies if the persistent volume claim has to be deleted when the cluster is un-deployed.")
    @JsonProperty(defaultValue = "false")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isDeleteClaim() {
        return deleteClaim;
    }

    public void setDeleteClaim(boolean deleteClaim) {
        this.deleteClaim = deleteClaim;
    }

    @Description("Overrides for individual brokers. " +
            "The `overrides` field allows to specify a different configuration for different brokers.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<PersistentClaimStorageOverride> getOverrides() {
        return overrides;
    }

    public void setOverrides(List<PersistentClaimStorageOverride> overrides) {
        this.overrides = overrides;
    }
}
