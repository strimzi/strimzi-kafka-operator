package io.strimzi.controller.cluster;

import java.util.HashMap;
import java.util.Map;

public class ClusterControllerConfig {

    public static final String STRIMZI_NAMESPACE = "STRIMZI_NAMESPACE";
    public static final String STRIMZI_CONFIGMAP_LABELS = "STRIMZI_CONFIGMAP_LABELS";
    public static final String STRIMZI_FULL_RECONCILIATION_INTERVAL = "STRIMZI_FULL_RECONCILIATION_INTERVAL";

    private static final long DEFAULT_FULL_RECONCILIATION_INTERVAL = 120000; // in ms (2 minutes)

    private Map<String, String> labels;
    private String namespace;
    private long reconciliationInterval;

    public ClusterControllerConfig(String namespace, Map<String, String> labels, long reconciliationInterval) {
        this.namespace = namespace;
        this.labels = labels;
        this.reconciliationInterval = reconciliationInterval;
    }

    public ClusterControllerConfig(String namespace, Map<String, String> labels) {
        this(namespace, labels, DEFAULT_FULL_RECONCILIATION_INTERVAL);
    }

    public static ClusterControllerConfig fromEnv() {

        String namespace = System.getenv(ClusterControllerConfig.STRIMZI_NAMESPACE);
        String stringLabels = System.getenv(ClusterControllerConfig.STRIMZI_CONFIGMAP_LABELS);
        long reconciliationInterval = DEFAULT_FULL_RECONCILIATION_INTERVAL;

        String reconciliationIntervalEnvVar = System.getenv(ClusterControllerConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL);
        if (reconciliationIntervalEnvVar != null) {
            reconciliationInterval = Long.valueOf(reconciliationIntervalEnvVar);
        }

        Map<String, String> labelsMap = new HashMap<>();

        String[] labels = stringLabels.split(",");
        for (String label : labels) {
            String[] fields = label.split("=");
            labelsMap.put(fields[0].trim(), fields[1].trim());
        }

        return new ClusterControllerConfig(namespace, labelsMap, reconciliationInterval);
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    public void setLabels(Map<String, String> labels) {
        this.labels = labels;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public long getReconciliationInterval() {
        return reconciliationInterval;
    }

    public void setReconciliationInterval(long reconciliationInterval) {
        this.reconciliationInterval = reconciliationInterval;
    }
}
