package io.strimzi.controller.cluster;

import java.util.HashMap;
import java.util.Map;

/**
 * Cluster Controller configuration
 */
public class ClusterControllerConfig {

    public static final String STRIMZI_NAMESPACE = "STRIMZI_NAMESPACE";
    public static final String STRIMZI_CONFIGMAP_LABELS = "STRIMZI_CONFIGMAP_LABELS";
    public static final String STRIMZI_FULL_RECONCILIATION_INTERVAL = "STRIMZI_FULL_RECONCILIATION_INTERVAL";

    private static final long DEFAULT_FULL_RECONCILIATION_INTERVAL = 120000; // in ms (2 minutes)

    private Map<String, String> labels;
    private String namespace;
    private long reconciliationInterval;

    /**
     * Constructor
     *
     * @param namespace namespace in which the controller will run and create resources
     * @param labels    labels used for watching the cluster ConfigMap
     * @param reconciliationInterval    specify every how many milliseconds the reconciliation runs
     */
    public ClusterControllerConfig(String namespace, Map<String, String> labels, long reconciliationInterval) {
        this.namespace = namespace;
        this.labels = labels;
        this.reconciliationInterval = reconciliationInterval;
    }

    /**
     * Constructor which provide a configuration with a default (120000 ms) reconciliation interval
     *
     * @param namespace namespace in which the controller will run and create resources
     * @param labels    labels used for watching the cluster ConfigMap
     */
    public ClusterControllerConfig(String namespace, Map<String, String> labels) {
        this(namespace, labels, DEFAULT_FULL_RECONCILIATION_INTERVAL);
    }

    /**
     * Loads configuration parameters from related environment variables
     *
     * @return  Cluster Controller configuration instance
     */
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
     * @return  namespace in which the controller runs and creates resources
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Set the namespace in which the controller runs and creates resources
     *
     * @param namespace namespace in which the controller runs and creates resources
     */
    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    /**
     * @return  how many milliseconds the reconciliation runs
     */
    public long getReconciliationInterval() {
        return reconciliationInterval;
    }

    /**
     * Set how many milliseconds the reconciliation runs
     *
     * @param reconciliationInterval    how many milliseconds the reconciliation runs
     */
    public void setReconciliationInterval(long reconciliationInterval) {
        this.reconciliationInterval = reconciliationInterval;
    }

    @Override
    public String toString() {
        return "ClusterControllerConfig(" +
                "namespace=" + namespace +
                ",labels=" + labels +
                ",reconciliationInterval=" + reconciliationInterval +
                ")";
    }
}
