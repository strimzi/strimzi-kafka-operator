/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.status;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Represents a status of the Kafka MirrorMaker resource
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"conditions", "observedGeneration"})
@EqualsAndHashCode
@ToString(callSuper = true)
public class KafkaMirrorMakerStatus extends Status {
    private static final long serialVersionUID = 1L;

    private int replicas;
    private LabelSelector podSelector;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Description("The current number of pods being used to provide this resource.")
    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @KubeLink(group = "meta", version = "v1", kind = "labelselector")
    @Description("Label selector for pods providing this resource.")
    public LabelSelector getPodSelector() {
        return podSelector;
    }

    public void setPodSelector(LabelSelector podSelector) {
        this.podSelector = podSelector;
    }
}
