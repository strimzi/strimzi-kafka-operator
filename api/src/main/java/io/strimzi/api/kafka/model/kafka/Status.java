/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Represents a generic status which can be used across different resources
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
@ToString
public abstract class Status implements UnknownPropertyPreserving, Serializable {
    private List<Condition> conditions;
    private long observedGeneration;
    private Map<String, Object> additionalProperties;

    @Description("List of status conditions")
    public List<Condition> getConditions() {
        return conditions;
    }

    public void setConditions(List<Condition> conditions) {
        this.conditions = conditions;
    }

    private List<Condition> prepareConditionsUpdate() {
        List<Condition> oldConditions = getConditions();
        List<Condition> newConditions = oldConditions != null ? new ArrayList<>(oldConditions) : new ArrayList<>(0);
        return newConditions;
    }

    public void addCondition(Condition condition) {
        List<Condition> newConditions = prepareConditionsUpdate();
        newConditions.add(condition);
        setConditions(Collections.unmodifiableList(newConditions));
    }

    public void addConditions(Collection<Condition> conditions) {
        List<Condition> newConditions = prepareConditionsUpdate();
        newConditions.addAll(conditions);
        setConditions(Collections.unmodifiableList(newConditions));
    }

    @Description("The generation of the CRD that was last reconciled by the operator.")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public long getObservedGeneration() {
        return observedGeneration;
    }

    public void setObservedGeneration(long observedGeneration) {
        this.observedGeneration = observedGeneration;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : emptyMap();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(1);
        }
        this.additionalProperties.put(name, value);
    }
}
