/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.storage;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.strimzi.crdgenerator.annotations.Pattern;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

/**
 * Representation for ephemeral storage.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
public class EphemeralStorage extends SingleVolumeStorage {

    private static final long serialVersionUID = 1L;

    private Integer id;

    private String sizeLimit;

    @Description("Must be `" + TYPE_EPHEMERAL + "`")
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

    @Pattern("^([0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$")
    @Description("When type=ephemeral, defines the total amount of local storage required for this EmptyDir volume (for example 1Gi).")
    public String getSizeLimit() {
        return sizeLimit;
    }

    public void setSizeLimit(String sizeLimit) {
        this.sizeLimit = sizeLimit;
    }
}
