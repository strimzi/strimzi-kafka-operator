package io.enmasse.barnabas.controller.cluster;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.DoneableStatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
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
      CREATE sub-blocks
     */
    public VolumeMount createVolumeMount(String name, String path) {
        log.trace("Creating volume mount {} with path {}", name, path);
        return new VolumeMountBuilder()
                .withName(name)
                .withMountPath(path)
                .build();
    }

    public ContainerPort createContainerPort(String name, int port) {
        log.trace("Creating container port {} named {}", port, name);
        return new ContainerPortBuilder()
                .withName(name)
                .withProtocol("TCP")
                .withContainerPort(port)
                .build();
    }

    public ServicePort createServicePort(String name, int port, int targetPort) {
        log.trace("Creating service port {} with target port {} named {}", port, targetPort, name);
        return new ServicePortBuilder()
                .withName(name)
                .withProtocol("TCP")
                .withPort(port)
                .withNewTargetPort(targetPort)
                .build();
    }

    public Volume createEmptyDirVolume(String name) {
        log.trace("Creating emptyDir volume named {}", name);
        return new VolumeBuilder()
                .withName(name)
                .withNewEmptyDir()
                .endEmptyDir()
                .build();
    }

    public Probe createExecProbe(String command, int initialDelay, int timeout) {
        log.trace("Creating exec probe with command {}, initial delay {} and timeout {}", command, initialDelay, timeout);
        return new ProbeBuilder().withNewExec()
                .withCommand(command)
                .endExec()
                .withInitialDelaySeconds(initialDelay)
                .withTimeoutSeconds(timeout)
                .build();
    }

    /*
      CREATE functions
     */

    public void createService(String namespace, Service svc) {
        log.debug("Creating service {}", svc.getMetadata().getName());
        client.services().inNamespace(namespace).createOrReplace(svc);
    }

    public void createStatefulSet(String namespace, StatefulSet ss) {
        log.debug("Creating stateful set {}", ss.getMetadata().getName());
        client.apps().statefulSets().inNamespace(namespace).createOrReplace(ss);
    }

    /*
      GET functions
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

    public Service getService(String namespace, String name)    {
        return getServiceResource(namespace, name).get();
    }

    public Resource<Service, DoneableService> getServiceResource(String namespace, String name)    {
        return client.services().inNamespace(namespace).withName(name);
    }

    public List<ConfigMap> getConfigmaps(String namespace, Map<String, String> labels) {
        return client.configMaps().inNamespace(namespace).withLabels(labels).list().getItems();
    }

    /*
      DELETE functions
     */
    public void deleteService(String namespace, String name) {
        if (serviceExists(namespace, name)) {
            log.debug("Deleting Zookeeper service {}", name);
            getServiceResource(namespace, name).delete();
        }
    }

    public void deleteStatefulSet(String namespace, String name) {
        if (statefulSetExists(namespace, name)) {
            log.debug("Deleting Zookeeper stateful set {}", name);
            getStatefulSetResource(namespace, name).delete();
        }
    }

    /*
      EXISTS functions
     */
    public boolean statefulSetExists(String namespace, String name) {
        return getStatefulSet(namespace, name) == null ? false : true;
    }

    public boolean serviceExists(String namespace, String name) {
        return getService(namespace, name) == null ? false : true;
    }
}
