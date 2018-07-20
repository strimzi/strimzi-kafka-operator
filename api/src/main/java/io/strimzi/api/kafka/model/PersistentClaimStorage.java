/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;

import java.util.Map;

/**
 * Representation for persistent claim-based storage.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = true,
        builderPackage = "io.strimzi.api.kafka.model"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PersistentClaimStorage extends Storage {

    private static final long serialVersionUID = 1L;

    private String size;
    private String storageClass;
    private Map<String, String> selector;
    private boolean deleteClaim;

    @Description("Must be `" + TYPE_PERSISTENT_CLAIM + "`")
    @Override
    public String getType() {
        return TYPE_PERSISTENT_CLAIM;
    }

    @Description("When type=persistent-claim, defines the size of the persistent volume claim (i.e 1Gi). " +
            "Mandatory when type=persistent-claim.")
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
            "It contains a matchLabels field which defines an inner JSON object with " +
            "key:value representing labels for selecting such a volume.")
    public Map<String, String> getSelector() {
        return selector;
    }

    public void setSelector(Map<String, String> selector) {
        this.selector = selector;
    }

    @Description("Specifies if the persistent volume claim has to be deleted when the cluster is un-deployed.")
    @JsonProperty(defaultValue = "false")
    public boolean isDeleteClaim() {
        return deleteClaim;
    }

    public void setDeleteClaim(boolean deleteClaim) {
        this.deleteClaim = deleteClaim;
    }
}
