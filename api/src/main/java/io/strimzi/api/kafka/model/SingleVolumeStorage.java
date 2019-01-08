/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.strimzi.crdgenerator.annotations.Description;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type"
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = EphemeralStorage.class, name = Storage.TYPE_EPHEMERAL),
        @JsonSubTypes.Type(value = PersistentClaimStorage.class, name = Storage.TYPE_PERSISTENT_CLAIM)}
)
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class SingleVolumeStorage extends Storage {

    private static final long serialVersionUID = 1L;

    @Override
    @Description("Storage type, must be either 'ephemeral' or 'persistent-claim'.")
    public abstract String getType();
}
