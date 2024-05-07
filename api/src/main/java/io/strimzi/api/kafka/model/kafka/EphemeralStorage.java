/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.strimzi.crdgenerator.annotations.Pattern;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Representation for ephemeral storage.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"id", "sizeLimit", "type", "kraftMetadata"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class EphemeralStorage extends SingleVolumeStorage {
    private String sizeLimit;

    @Description("Must be `" + TYPE_EPHEMERAL + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_EPHEMERAL;
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

    @Pattern(Constants.MEMORY_REGEX)
    @Description("When type=ephemeral, defines the total amount of local storage required for this EmptyDir volume (for example 1Gi).")
    public String getSizeLimit() {
        return sizeLimit;
    }

    public void setSizeLimit(String sizeLimit) {
        this.sizeLimit = sizeLimit;
    }

    @Override
    @Description("Specifies whether this volume should be used for storing KRaft metadata. " +
            "This property is optional. " +
            "When set, the only currently supported value is `shared`. " +
            "At most one volume can have this property set.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public KRaftMetadataStorage getKraftMetadata() {
        return super.getKraftMetadata();
    }

    @Override
    public void setKraftMetadata(KRaftMetadataStorage kraftMetadata) {
        super.setKraftMetadata(kraftMetadata);
    }
}
