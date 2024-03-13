/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

/**
 * Representation for JBOD storage.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "volumes"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class JbodStorage extends Storage {

    private static final long serialVersionUID = 1L;

    private List<SingleVolumeStorage> volumes;

    @Description("Must be `" + TYPE_JBOD + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_JBOD;
    }

    @Description("List of volumes as Storage objects representing the JBOD disks array")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<SingleVolumeStorage> getVolumes() {
        return volumes;
    }

    public void setVolumes(List<SingleVolumeStorage> volumes) {
        this.volumes = volumes;
    }
}
