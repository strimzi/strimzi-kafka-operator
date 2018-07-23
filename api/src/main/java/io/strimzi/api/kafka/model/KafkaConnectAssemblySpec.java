/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import io.vertx.core.cli.annotations.DefaultValue;

import java.util.HashMap;
import java.util.Map;

@Buildable(
        editableEnabled = false,
        generateBuilderPackage = true,
        builderPackage = "io.strimzi.api.kafka.model"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "replicas", "image",
        "livenessProbe", "readinessProbe", "jvmOptions", "affinity", "metrics"})
public class KafkaConnectAssemblySpec extends ReplicatedJvmPods {

    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_IMAGE =
            System.getenv().getOrDefault("STRIMZI_DEFAULT_KAFKA_CONNECT_IMAGE", "strimzi/kafka-connect:latest");

    public static final String FORBIDDEN_PREFIXES = "ssl., sasl., security., listeners, plugin.path, rest.";

    private Map<String, Object> config = new HashMap<>(0);

    private Logging logging;

    @Override
    @Description("The number of pods in the Kafka Connect group.")
    @DefaultValue("3")
    public int getReplicas() {
        return super.getReplicas();
    }

    @Description("The Kafka Connect configuration. Properties with the following prefixes cannot be set: " + FORBIDDEN_PREFIXES)
    @JsonProperty(required = true)
    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    @Description("Logging configuration for Kafka Connect")
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public Logging getLogging() {
        return logging;
    }

    public void setLogging(Logging logging) {
        this.logging = logging;
    }
}
