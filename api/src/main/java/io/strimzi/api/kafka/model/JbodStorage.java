/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * Representation for JBOD storage.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JbodStorage extends Storage {

    private static final long serialVersionUID = 1L;
    static final String MISSING_ID = "Volumes under JBOD storage type have to have 'id' property";

    private List<SingleVolumeStorage> volumes;

    @Description("Must be `" + TYPE_JBOD + "`")
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

    /**
     * {@inheritDoc}
     */
    @Override
    public String invalidityReason() {
        for (SingleVolumeStorage volume : volumes) {
            String reason = volume.invalidityReason();
            if (reason != null) {
                return reason;
            }
            if (volume.getId() == null) {
                return MISSING_ID;
            }
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsPersistentStorage() {
        return volumes.stream().anyMatch(Storage::containsPersistentStorage);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void iteratePersistentClaimStorage(BiConsumer<PersistentClaimStorage, String> consumer, String name) {
        volumes.forEach(svs -> svs.iteratePersistentClaimStorage(consumer, svs.suffixed(name)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void iterateEphemeralStorage(BiConsumer<EphemeralStorage, String> consumer, String name) {
        volumes.forEach(svs -> svs.iterateEphemeralStorage(consumer, svs.suffixed(name)));
    }

}
