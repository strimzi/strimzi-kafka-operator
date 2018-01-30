package io.strimzi.controller.cluster;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DoneableDeployment;
import io.fabric8.kubernetes.api.model.extensions.DoneableStatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.*;
import io.fabric8.openshift.client.OpenShiftClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class K8SUtils {
    private static final Logger log = LoggerFactory.getLogger(K8SUtils.class.getName());

    private final KubernetesClient client;

    public K8SUtils(KubernetesClient client) {
        this.client = client;
    }

    public KubernetesClient getKubernetesClient() {
        return client;
    }

    /**
     * @return  If the current cluster is an OpenShift one, returns true. If its Kubernetes, returns false.
     */
    public boolean isOpenShift() {
        return this.client.isAdaptable(OpenShiftClient.class);
    }

    /*
      GET methods
     */

    public List<StatefulSet> getStatefulSets(String namespace, Map<String, String> labels) {
        return client.apps().statefulSets().inNamespace(namespace).withLabels(labels).list().getItems();
    }

    public List<Deployment> getDeployments(String namespace, Map<String, String> labels) {
        return client.extensions().deployments().inNamespace(namespace).withLabels(labels).list().getItems();
    }

    public List<ConfigMap> getConfigmaps(String namespace, Map<String, String> labels) {
        return client.configMaps().inNamespace(namespace).withLabels(labels).list().getItems();
    }

}
