/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;

import java.util.HashMap;
import java.util.Map;

/**
 * JMX Prometheus metrics config
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonPropertyOrder({"type", "valueFrom"})
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JmxPrometheusExporterMetrics extends MetricsConfig {

    private static final long serialVersionUID = 1L;

    public static final String TYPE_JMX_EXPORTER = "jmxPrometheusExporter";

    private ExternalConfigurationMetrics valueFrom;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("ConfigMap where Prometheus JMX Exporter configuration is stored. " +
            "See https://github.com/prometheus/jmx_exporter for details of the structure of this configuration.")
    @JsonProperty(required = true)
    public ExternalConfigurationMetrics getValueFrom() {
        return valueFrom;
    }

    public void setValueFrom(ExternalConfigurationMetrics valueFrom) {
        this.valueFrom = valueFrom;
    }

    @Description("Must be `" + TYPE_JMX_EXPORTER + "`")
    @Override
    public String getType() {
        return TYPE_JMX_EXPORTER;
    }

}
