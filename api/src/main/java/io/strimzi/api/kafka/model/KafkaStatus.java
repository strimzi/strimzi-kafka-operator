/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * status:
 *   conditions:
 *   - lastTransitionTime: 2019-04-08T09:13:27Z
 *     status: "False"
 *     type: ZookeeperRollingRestart
 *   - lastTransitionTime: 2019-04-08T09:13:27Z
 *     status: "True"
 *     type: KafkaRollingRestart
 *     reason: "KafkaUpgradePhase1"
 *   - lastTransitionTime: 2019-04-08T09:13:27Z
 *     status: "False"
 *     type: EntityOperatorRestarting
 *   listeners:
 *     plain:
 *       host: foobar
 *       port: 123
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
public class KafkaStatus implements UnknownPropertyPreserving, Serializable {

    private boolean ready;
    private ListenersStatus listeners;
    private List<Condition> conditions;
    private Map<String, Object> additionalProperties;

    @Description("")
    public ListenersStatus getListeners() {
        return listeners;
    }

    public void setListeners(ListenersStatus listeners) {
        this.listeners = listeners;
    }

    @Description("")
    public List<Condition> getConditions() {
        return conditions;
    }

    public void setConditions(List<Condition> conditions) {
        this.conditions = conditions;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : emptyMap();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>();
        }
        this.additionalProperties.put(name, value);
    }
}
