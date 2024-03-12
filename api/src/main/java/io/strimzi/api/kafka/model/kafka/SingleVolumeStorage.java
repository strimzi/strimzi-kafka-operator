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

    private static final long serialVersionUID = 1L;

    private Integer id;

    @Override
    @Description("Storage type, must be either 'ephemeral' or 'persistent-claim'.")
    public abstract String getType();

    @Description("Storage identification number. It is mandatory only for storage volumes defined in a storage of type 'jbod'")
    @Minimum(0)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }
}
