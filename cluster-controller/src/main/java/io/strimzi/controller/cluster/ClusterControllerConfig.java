/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster;

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
    public static final String STRIMZI_FULL_RECONCILIATION_INTERVAL = "STRIMZI_FULL_RECONCILIATION_INTERVAL";

    public static final long DEFAULT_FULL_RECONCILIATION_INTERVAL = 120_000; // in ms (2 minutes)

    private Map<String, String> labels;
    private Set<String> namespaces;
    private long reconciliationInterval;

    /**
     * Constructor
     *
     * @param namespaces namespace in which the controller will run and create resources
     * @param labels    labels used for watching the cluster ConfigMap
     * @param reconciliationInterval    specify every how many milliseconds the reconciliation runs
     */
    public ClusterControllerConfig(Set<String> namespaces, Map<String, String> labels, long reconciliationInterval) {
        this.namespaces = unmodifiableSet(new HashSet<>(namespaces));
        this.labels = labels;
        this.reconciliationInterval = reconciliationInterval;
    }

    /**
     * Constructor which provide a configuration with a default (120000 ms) reconciliation interval
     *
     * @param namespaces namespace in which the controller will run and create resources
     * @param labels    labels used for watching the cluster ConfigMap
     */
    public ClusterControllerConfig(Set<String> namespaces, Map<String, String> labels) {
        this(namespaces, labels, DEFAULT_FULL_RECONCILIATION_INTERVAL);
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
        Long reconciliationInterval = DEFAULT_FULL_RECONCILIATION_INTERVAL;

        String reconciliationIntervalEnvVar = map.get(ClusterControllerConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL);
        if (reconciliationIntervalEnvVar != null) {
            reconciliationInterval = Long.valueOf(reconciliationIntervalEnvVar);
        }

        Map<String, String> labelsMap = new HashMap<>();

        String[] labels = stringLabels.split(",");
        for (String label : labels) {
            String[] fields = label.split("=");
            labelsMap.put(fields[0].trim(), fields[1].trim());
        }

        return new ClusterControllerConfig(namespaces, labelsMap, reconciliationInterval);
    }

    /**
     * @return  labels used for watching the cluster ConfigMap
     */
    public Map<String, String> getLabels() {
        return labels;
    }

    /**
     * Set the labels used for watching the cluster ConfigMap
     *
     * @param labels    labels used for watching the cluster ConfigMap
     */
    public void setLabels(Map<String, String> labels) {
        this.labels = labels;
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
    public long getReconciliationInterval() {
        return reconciliationInterval;
    }

    @Override
    public String toString() {
        return "ClusterControllerConfig(" +
                "namespaces=" + namespaces +
                ",labels=" + labels +
                ",reconciliationInterval=" + reconciliationInterval +
                ")";
    }
}
