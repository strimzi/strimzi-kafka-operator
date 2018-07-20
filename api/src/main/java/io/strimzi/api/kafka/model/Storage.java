/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.strimzi.crdgenerator.annotations.Description;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Abstract baseclass for different representations of storage, discriminated by {@link #getType() type}.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "type"
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = EphemeralStorage.class, name = Storage.TYPE_EPHEMERAL),
        @JsonSubTypes.Type(value = PersistentClaimStorage.class, name = Storage.TYPE_PERSISTENT_CLAIM)}
)
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class Storage implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String TYPE_EPHEMERAL = "ephemeral";
    public static final String TYPE_PERSISTENT_CLAIM = "persistent-claim";
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Storage type, must be either 'ephemeral' or 'persistent-claim'.")
    @JsonIgnore
    public abstract String getType();

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    public static boolean deleteClaim(Storage storage) {
        return storage instanceof PersistentClaimStorage
                && ((PersistentClaimStorage) storage).isDeleteClaim();
    }

    public static String storageClass(Storage storage) {
        return storage instanceof PersistentClaimStorage ?
                ((PersistentClaimStorage) storage).getStorageClass() : null;
    }
}

