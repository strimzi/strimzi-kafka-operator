package io.strimzi.controller.cluster;

import java.util.HashMap;
import java.util.Map;

public class ClusterControllerConfig {

    public static final String STRIMZI_NAMESPACE = "STRIMZI_NAMESPACE";
    public static final String STRIMZI_CONFIGMAP_LABELS = "STRIMZI_CONFIGMAP_LABELS";

    private Map<String, String> labels;
    private String namespace;

    public ClusterControllerConfig(String namespace, Map<String, String> labels) {
        this.namespace = namespace;
        this.labels = labels;
    }

    public static ClusterControllerConfig fromEnv() {

        String namespace = System.getenv(ClusterControllerConfig.STRIMZI_NAMESPACE);
        String stringLabels = System.getenv(ClusterControllerConfig.STRIMZI_CONFIGMAP_LABELS);

        Map<String, String> labelsMap = new HashMap<>();

        String[] labels = stringLabels.split(",");
        for (String label : labels) {
            String[] fields = label.split("=");
            labelsMap.put(fields[0].trim(), fields[1].trim());
        }

        return new ClusterControllerConfig(namespace, labelsMap);
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
}
