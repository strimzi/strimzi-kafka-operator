/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.metrics;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Strimzi Metrics Reporter.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonPropertyOrder({"type", "values"})
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class StrimziReporterMetrics extends MetricsConfig {
    public static final String TYPE_STRIMZI_REPORTER_METRICS = "strimziMetricsReporter";

    private StrimziReporterValues values;

    @Description("Must be `" + TYPE_STRIMZI_REPORTER_METRICS + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_STRIMZI_REPORTER_METRICS;
    }

    @Description("Configuration values for the Strimzi Metrics Reporter.")
    public StrimziReporterValues getValues() {
        return values;
    }

    public void setValues(StrimziReporterValues values) {
        this.values = values;
    }
}
