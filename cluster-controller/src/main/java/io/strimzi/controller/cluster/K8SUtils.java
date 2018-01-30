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
    public StatefulSet getStatefulSet(String namespace, String name)    {
        return getStatefulSetResource(namespace, name).get();
    }

    public RollableScalableResource<StatefulSet, DoneableStatefulSet> getStatefulSetResource(String namespace, String name)    {
        return client.apps().statefulSets().inNamespace(namespace).withName(name);
    }

    public List<StatefulSet> getStatefulSets(String namespace, Map<String, String> labels) {
        return client.apps().statefulSets().inNamespace(namespace).withLabels(labels).list().getItems();
    }

    public ScalableResource<Deployment, DoneableDeployment> getDeploymentResource(String namespace, String name)    {
        return client.extensions().deployments().inNamespace(namespace).withName(name);
    }

    public List<Deployment> getDeployments(String namespace, Map<String, String> labels) {
        return client.extensions().deployments().inNamespace(namespace).withLabels(labels).list().getItems();
    }

    public Pod getPod(String namespace, String name)    {
        return getPodResource(namespace, name).get();
    }

    public PodResource<Pod, DoneablePod> getPodResource(String namespace, String name)    {
        return client.pods().inNamespace(namespace).withName(name);
    }

    public List<ConfigMap> getConfigmaps(String namespace, Map<String, String> labels) {
        return client.configMaps().inNamespace(namespace).withLabels(labels).list().getItems();
    }

    /*
      DELETE methods
     */
    public void deletePod(String namespace, String name) {
        if (podExists(namespace, name)) {
            log.debug("Deleting pod {}", name);
            getPodResource(namespace, name).delete();
        }
    }

    /*
      SCALE methods
     */
    public void scale(ScalableResource res, int replicas, boolean wait)    {
        res.scale(replicas, wait);
    }

    /*
      WATCH methods
     */

    public Watch createPodWatch(String namespace, String name, Watcher watcher) {
        return client.pods().inNamespace(namespace).withName(name).watch(watcher);
    }

    /*
      EXISTS methods
     */
    public boolean podExists(String namespace, String name) {
        return getPod(namespace, name) == null ? false : true;
    }

    /*
      READY methods
     */
    public boolean isPodReady(String namespace, String name) {
        return getPodResource(namespace, name).isReady();
    }

}
