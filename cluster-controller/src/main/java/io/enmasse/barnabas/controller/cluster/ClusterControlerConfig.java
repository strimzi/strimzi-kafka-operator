package io.enmasse.barnabas.controller.cluster;

import java.util.HashMap;
import java.util.Map;

public class ClusterControlerConfig {
    private Map<String, String> labels;
    private String namespace;

    public ClusterControlerConfig(String namespace, Map<String, String> labels) {
        this.namespace = namespace;
        this.labels = labels;
    }

    public static ClusterControlerConfig fromEnv() {
        String namespace = System.getenv("BARNABAS_CONTROLLER_NAMESPACE");
        String stringLabels = System.getenv("BARNABAS_CONTROLLER_LABELS");

        Map<String, String> labelsMap = new HashMap<>();

        String[] labels = stringLabels.split(",");
        for (String label : labels) {
            String[] fields = label.split("=");
            labelsMap.put(fields[0].trim(), fields[1].trim());
        }

        return new ClusterControlerConfig(namespace, labelsMap);
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
