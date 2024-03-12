/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.podset;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.Spec;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;
import java.util.Map;

@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"selector", "pods"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class StrimziPodSetSpec extends Spec {
    private static final long serialVersionUID = 1L;

    private LabelSelector selector;
    private List<Map<String, Object>> pods;

    @Description("Selector is a label query which matches all the pods managed by this `StrimziPodSet`. " +
            "Only `matchLabels` is supported. " +
            "If `matchExpressions` is set, it will be ignored.")
    @KubeLink(group = "meta", version = "v1", kind = "labelselector")
    @JsonProperty(required = true)
    public LabelSelector getSelector() {
        return selector;
    }

    public void setSelector(LabelSelector selector) {
        this.selector = selector;
    }

    @Description("The Pods managed by this StrimziPodSet.")
    @KubeLink(group = "core", version = "v1", kind = "pods")
    @JsonProperty(required = true)
    public List<Map<String, Object>> getPods() {
        return pods;
    }

    public void setPods(List<Map<String, Object>> pods) {
        this.pods = pods;
    }
}
