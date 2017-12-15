package io.enmasse.barnabas.controller.cluster;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DoneableDeployment;
import io.fabric8.kubernetes.api.model.extensions.DoneableStatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.*;
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

    /*
      CREATE methods
     */
    public void createService(String namespace, Service svc) {
        log.info("Creating service {}", svc.getMetadata().getName());
        client.services().inNamespace(namespace).createOrReplace(svc);
    }

    public void createService(Service svc) {
        log.info("Creating service {}", svc.getMetadata().getName());
        client.services().createOrReplace(svc);
    }

    public void createStatefulSet(String namespace, StatefulSet ss) {
        log.info("Creating stateful set {}", ss.getMetadata().getName());
        client.apps().statefulSets().inNamespace(namespace).createOrReplace(ss);
    }

    public void createStatefulSet(StatefulSet ss) {
        log.info("Creating stateful set {}", ss.getMetadata().getName());
        client.apps().statefulSets().createOrReplace(ss);
    }

    public void createDeployment(String namespace, Deployment dep) {
        log.info("Creating deployment {}", dep.getMetadata().getName());
        client.extensions().deployments().inNamespace(namespace).createOrReplace(dep);
    }

    public void createDeployment(Deployment dep) {
        log.info("Creating deployment {}", dep.getMetadata().getName());
        client.extensions().deployments().createOrReplace(dep);
    }

    public void createConfigMap(ConfigMap cm) {
        log.info("Creating configmap {}", cm.getMetadata().getName());
        client.configMaps().createOrReplace(cm);
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

    public Deployment getDeployment(String namespace, String name)    {
        return getDeploymentResource(namespace, name).get();
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

    public Service getService(String namespace, String name)    {
        return getServiceResource(namespace, name).get();
    }

    public Resource<Service, DoneableService> getServiceResource(String namespace, String name)    {
        return client.services().inNamespace(namespace).withName(name);
    }

    public ConfigMap getConfigmap(String namespace, String name) {
        return client.configMaps().inNamespace(namespace).withName(name).get();
    }

    public List<ConfigMap> getConfigmaps(String namespace, Map<String, String> labels) {
        return client.configMaps().inNamespace(namespace).withLabels(labels).list().getItems();
    }

    /*
      DELETE methods
     */
    public void deleteService(String namespace, String name) {
        if (serviceExists(namespace, name)) {
            log.debug("Deleting service {}", name);
            getServiceResource(namespace, name).delete();
        }
    }

    public void deleteStatefulSet(String namespace, String name) {
        if (statefulSetExists(namespace, name)) {
            log.debug("Deleting stateful set {}", name);
            getStatefulSetResource(namespace, name).delete();
        }
    }

    public void deleteDeployment(String namespace, String name) {
        if (deploymentExists(namespace, name)) {
            log.debug("Deleting deployment {}", name);
            getDeploymentResource(namespace, name).delete();
        }
    }

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
      PATCH methods
     */
    public void patch(Patchable patchable, KubernetesResource patch)    {
        patchable.patch(patch);
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
    public boolean statefulSetExists(String namespace, String name) {
        return getStatefulSet(namespace, name) == null ? false : true;
    }

    public boolean deploymentExists(String namespace, String name) {
        return getDeployment(namespace, name) == null ? false : true;
    }

    public boolean serviceExists(String namespace, String name) {
        return getService(namespace, name) == null ? false : true;
    }

    public boolean podExists(String namespace, String name) {
        return getPod(namespace, name) == null ? false : true;
    }

    public boolean configMapExists(String namespace, String name) {
        return getConfigmap(namespace, name) == null ? false : true;
    }

    /*
      READY methods
     */
    public boolean isPodReady(String namespace, String name) {
        return getPodResource(namespace, name).isReady();
    }

}
