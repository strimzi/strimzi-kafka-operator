/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.mirrormaker2;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.RequiredInVersions;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

//@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "alias", "bootstrapServers", "groupId", "configStorageTopic", "statusStorageTopic",
    "offsetStorageTopic", "tls", "authentication", "config"})
@EqualsAndHashCode(callSuper = true)
@ToString
public class KafkaMirrorMaker2TargetClusterSpec extends KafkaMirrorMaker2ClusterSpec {
    private String groupId;
    private String configStorageTopic;
    private String statusStorageTopic;
    private String offsetStorageTopic;

    @Description("A unique ID that identifies the Connect cluster group.")
    @RequiredInVersions("v1beta2+")
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    @Description("The name of the Kafka topic where connector configurations are stored.")
    @RequiredInVersions("v1beta2+")
    public String getConfigStorageTopic() {
        return configStorageTopic;
    }

    public void setConfigStorageTopic(String configStorageTopic) {
        this.configStorageTopic = configStorageTopic;
    }

    @Description("The name of the Kafka topic where connector and task status are stored")
    @RequiredInVersions("v1beta2+")
    public String getStatusStorageTopic() {
        return statusStorageTopic;
    }

    public void setStatusStorageTopic(String statusStorageTopic) {
        this.statusStorageTopic = statusStorageTopic;
    }

    @Description("The name of the Kafka topic where source connector offsets are stored.")
    @RequiredInVersions("v1beta2+")
    public String getOffsetStorageTopic() {
        return offsetStorageTopic;
    }

    public void setOffsetStorageTopic(String offsetStorageTopic) {
        this.offsetStorageTopic = offsetStorageTopic;
    }
}
