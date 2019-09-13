/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.config.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * A model of a particular configuration parameter.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ConfigModel {
    private Scope scope;
    private Type type;
    private Number minimum;
    private Number maximum;
    private List<String> items;
    @JsonProperty("enum")
    private List<String> values;
    private String pattern;

    /**
     * @return The scope of the parameter.
     */
    public Scope getScope() {
        return scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    /**
     * @return Inclusive minimum for a parameter of numeric type.
     */
    public Number getMinimum() {
        return minimum;
    }

    public void setMinimum(Number minimum) {
        this.minimum = minimum;
    }

    /**
     * @return Inclusive maximum for a parameter of numeric type.
     */
    public Number getMaximum() {
        return maximum;
    }

    public void setMaximum(Number maximum) {
        this.maximum = maximum;
    }

    /**
     * @return The allowed items for the parameter value, for parameters of list type.
     */
    public List<String> getItems() {
        return items;
    }

    public void setItems(List<String> items) {
        this.items = items;
    }

    /**
     * @return The allowed values for the parameter value, for parameters of string type.
     */
    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    /**
     * @return A regular expression which values must match, for parameters of string type.
     */
    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }
}
