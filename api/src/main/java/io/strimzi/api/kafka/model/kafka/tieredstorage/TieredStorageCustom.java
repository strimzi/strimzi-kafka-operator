/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.api.kafka.model.kafka.tieredstorage;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Configures a tieredStorage to use custom type.
 */
@DescriptionFile
@Buildable(
    editableEnabled = false,
    builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "remoteStorageManager"})
@EqualsAndHashCode
@ToString
public class TieredStorageCustom extends TieredStorage {
    private static final long serialVersionUID = 1L;

    private RemoteStorageManager remoteStorageManager;

    @Description("Must be `" + TYPE_CUSTOM + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_CUSTOM;
    }

    @Description("Configuration for the Remote Storage Manager.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public RemoteStorageManager getRemoteStorageManager() {
        return remoteStorageManager;
    }

    public void setRemoteStorageManager(RemoteStorageManager remoteStorageManager) {
        this.remoteStorageManager = remoteStorageManager;
    }
}
