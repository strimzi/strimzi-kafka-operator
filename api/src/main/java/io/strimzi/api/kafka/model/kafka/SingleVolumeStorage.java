/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import lombok.EqualsAndHashCode;
import lombok.ToString;

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
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public abstract class SingleVolumeStorage extends Storage {
    private Integer id;
    private KRaftMetadataStorage kraftMetadata;

    @Override
    @Description("Storage type, must be either 'ephemeral' or 'persistent-claim'.")
    public abstract String getType();

    @Description("Storage identification number. Mandatory for storage volumes defined with a `jbod` storage type configuration.")
    @Minimum(0)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    @Description("Specifies whether this volume should be used for storing KRaft metadata. " +
            "This property is optional. " +
            "When set, the only currently supported value is `shared`. " +
            "At most one volume can have this property set.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public KRaftMetadataStorage getKraftMetadata() {
        return kraftMetadata;
    }

    public void setKraftMetadata(KRaftMetadataStorage kraftMetadata) {
        this.kraftMetadata = kraftMetadata;
    }
}
