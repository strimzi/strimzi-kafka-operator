package io.enmasse.barnabas.controller.cluster.resources;

import io.enmasse.barnabas.controller.cluster.K8SUtils;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractResource implements Resource {
    private static final Logger log = LoggerFactory.getLogger(AbstractResource.class.getName());

    private final ResourceId id;
    protected final String name;

    protected final K8SUtils k8s;
    protected final Vertx vertx;

    protected final String namespace;
    protected Map<String, String> labels = new HashMap<>();

    protected final int LOCK_TIMEOUT = 60000;

    protected AbstractResource(String namespace, String name, ResourceId id, Vertx vertx, K8SUtils k8s) {
        this.id = id;
        this.name = name;
        this.vertx = vertx;
        this.k8s = k8s;

        this.namespace = namespace;
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    public void setLabels(Map<String, String> newLabels) {
        newLabels.put("cluster-name", name);
        this.labels = new HashMap<String, String>(newLabels);
    }

    protected Map<String, String> getLabelsWithName() {
        return getLabelsWithName(name);
    }

    protected Map<String, String> getLabelsWithName(String name) {
        Map<String, String> labelsWithName = new HashMap<>(labels);
        labelsWithName.put("name", name);
        return labelsWithName;
    }

    public ResourceId getId() {
        return id;
    }
}
