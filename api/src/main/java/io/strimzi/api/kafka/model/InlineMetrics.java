/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;

import java.util.List;
import java.util.Map;

/**
 * Logging config is given inline with the resource
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonPropertyOrder({"type", "lowercaseOutputName", "rules"})
@JsonInclude(JsonInclude.Include.NON_NULL)
public class InlineMetrics extends Metrics {

    private static final long serialVersionUID = 1L;

    public static final String TYPE_INLINE = "inline";

    private List<Map<String, Object>> rules = null;
    private Boolean lowercaseOutputName = null;
    private Boolean lowercaseOutputLabelNames = null;

    @Description("Must be `" + TYPE_INLINE + "`")
    @Override
    public String getType() {
        return TYPE_INLINE;
    }

    @Description("A flag for lowercaseOutputName.")
    public Boolean getLowercaseOutputName() {
        return lowercaseOutputName;
    }

    public void setLowercaseOutputName(Boolean lowercaseOutputName) {
        this.lowercaseOutputName = lowercaseOutputName;
    }

    @Description("A flag for lowercaseOutputLabelNames.")
    public Boolean getLowercaseOutputLabelNames() {
        return lowercaseOutputLabelNames;
    }

    public void setLowercaseOutputLabelNames(Boolean lowercaseOutputLabelNames) {
        this.lowercaseOutputLabelNames = lowercaseOutputLabelNames;
    }

    @Description("A List of metrics rules.")
    public List<Map<String, Object>> getRules() {
        return rules;
    }

    public void setRules(List<Map<String, Object>> rules) {
        this.rules = rules;
    }
}
