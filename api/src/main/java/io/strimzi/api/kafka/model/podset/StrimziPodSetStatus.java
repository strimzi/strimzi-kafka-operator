/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.podset;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.kafka.Status;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Represents a status of the StrimziPodSet resource
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "conditions", "observedGeneration", "pods", "readyPods", "currentPods" })
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class StrimziPodSetStatus extends Status {
    private static final long serialVersionUID = 1L;

    private int pods;
    private int readyPods;
    private int currentPods;

    @Description("Number of pods managed by this `StrimziPodSet` resource.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public int getPods() {
        return pods;
    }

    public void setPods(int pods) {
        this.pods = pods;
    }

    @Description("Number of pods managed by this `StrimziPodSet` resource that are ready.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public int getReadyPods() {
        return readyPods;
    }

    public void setReadyPods(int readyPods) {
        this.readyPods = readyPods;
    }

    @Description("Number of pods managed by this `StrimziPodSet` resource that have the current revision.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public int getCurrentPods() {
        return currentPods;
    }

    public void setCurrentPods(int currentPods) {
        this.currentPods = currentPods;
    }
}
