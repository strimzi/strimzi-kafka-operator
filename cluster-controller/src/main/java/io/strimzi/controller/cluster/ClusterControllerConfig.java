/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster;

import io.strimzi.controller.cluster.resources.Labels;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSet;

/**
 * Cluster Controller configuration
 */
public class ClusterControllerConfig {

    public static final String STRIMZI_NAMESPACE = "STRIMZI_NAMESPACE";
    public static final String STRIMZI_CONFIGMAP_LABELS = "STRIMZI_CONFIGMAP_LABELS";
    public static final String STRIMZI_FULL_RECONCILIATION_INTERVAL_MS = "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS";
    public static final String STRIMZI_OPERATION_TIMEOUT_MS = "STRIMZI_OPERATION_TIMEOUT_MS";

    public static final long DEFAULT_FULL_RECONCILIATION_INTERVAL_MS = 120_000;
    public static final long DEFAULT_OPERATION_TIMEOUT_MS = 60_000;

    private final Labels labels;
    private final Set<String> namespaces;
    private final long reconciliationIntervalMs;
    private final long operationTimeoutMs;

    /**
     * Constructor
     *
     * @param namespaces namespace in which the controller will run and create resources
     * @param labels    labels used for watching the cluster ConfigMap
     * @param reconciliationIntervalMs    specify every how many milliseconds the reconciliation runs
     * @param operationTimeoutMs    timeout for internal operations specified in milliseconds
     */
    public ClusterControllerConfig(Set<String> namespaces, Labels labels, long reconciliationIntervalMs, long operationTimeoutMs) {
        this.namespaces = unmodifiableSet(new HashSet<>(namespaces));
        this.labels = labels;
        this.reconciliationIntervalMs = reconciliationIntervalMs;
        this.operationTimeoutMs = operationTimeoutMs;
    }

    /**
     * Constructor which provide a configuration with a default (120000 ms) reconciliation interval
     *
     * @param namespaces namespace in which the controller will run and create resources
     * @param labels    labels used for watching the cluster ConfigMap
     */
    public ClusterControllerConfig(Set<String> namespaces, Labels labels) {
        this(namespaces, labels, DEFAULT_FULL_RECONCILIATION_INTERVAL_MS, DEFAULT_OPERATION_TIMEOUT_MS);
    }

    /**
     * Loads configuration parameters from a related map
     *
     * @param map   map from which loading configuration parameters
     * @return  Cluster Controller configuration instance
     */
    public static ClusterControllerConfig fromMap(Map<String, String> map) {

        String namespacesList = map.get(ClusterControllerConfig.STRIMZI_NAMESPACE);
        Set<String> namespaces;
        if (namespacesList == null || namespacesList.isEmpty()) {
            throw new IllegalArgumentException(ClusterControllerConfig.STRIMZI_NAMESPACE + " cannot be null");
        } else {
            namespaces = new HashSet(asList(namespacesList.trim().split("\\s*,+\\s*")));
        }
        String stringLabels = map.get(ClusterControllerConfig.STRIMZI_CONFIGMAP_LABELS);
        if (stringLabels == null || stringLabels.isEmpty()) {
            throw new IllegalArgumentException(ClusterControllerConfig.STRIMZI_CONFIGMAP_LABELS + " cannot be null");
        }

        long reconciliationInterval = DEFAULT_FULL_RECONCILIATION_INTERVAL_MS;
        String reconciliationIntervalEnvVar = map.get(ClusterControllerConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS);
        if (reconciliationIntervalEnvVar != null) {
            reconciliationInterval = Long.parseLong(reconciliationIntervalEnvVar);
        }

        long operationTimeout = DEFAULT_OPERATION_TIMEOUT_MS;
        String operationTimeoutEnvVar = map.get(ClusterControllerConfig.STRIMZI_OPERATION_TIMEOUT_MS);
        if (operationTimeoutEnvVar != null) {
            operationTimeout = Long.parseLong(operationTimeoutEnvVar);
        }

        Map<String, String> labelsMap = new HashMap<>();

        String[] labels = stringLabels.split(",");
        for (String label : labels) {
            String[] fields = label.split("=");
            labelsMap.put(fields[0].trim(), fields[1].trim());
        }

        return new ClusterControllerConfig(namespaces, Labels.userLabels(labelsMap), reconciliationInterval, operationTimeout);
    }

    /**
     * @return  labels used for watching the cluster ConfigMap
     */
    public Labels getLabels() {
        return labels;
    }

    /**
     * @return  namespaces in which the controller runs and creates resources
     */
    public Set<String> getNamespaces() {
        return namespaces;
    }

    /**
     * @return  how many milliseconds the reconciliation runs
     */
    public long getReconciliationIntervalMs() {
        return reconciliationIntervalMs;
    }

    /**
     * @return  how many milliseconds should we wait for Kubernetes operations
     */
    public long getOperationTimeoutMs() {
        return operationTimeoutMs;
    }

    @Override
    public String toString() {
        return "ClusterControllerConfig(" +
                "namespaces=" + namespaces +
                ",labels=" + labels +
                ",reconciliationIntervalMs=" + reconciliationIntervalMs +
                ")";
    }
}
