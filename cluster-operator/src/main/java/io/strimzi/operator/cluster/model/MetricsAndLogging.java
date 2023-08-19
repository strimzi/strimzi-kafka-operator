/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;

/**
 * Holder record for ConfigMaps with Metrics and Logging configuration
 */
public record MetricsAndLogging(ConfigMap metricsCm, ConfigMap loggingCm) { }
