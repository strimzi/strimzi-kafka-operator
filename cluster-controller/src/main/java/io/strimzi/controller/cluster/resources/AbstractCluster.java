/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.extensions.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetUpdateStrategyBuilder;
import io.strimzi.controller.cluster.ClusterController;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractCluster {

    protected static final Logger log = LoggerFactory.getLogger(AbstractCluster.class.getName());

    private static final String VOLUME_MOUNT_HACK_IMAGE = "busybox";
    private static final String VOLUME_MOUNT_HACK_NAME = "volume-mount-hack";
    private static final Long VOLUME_MOUNT_HACK_GROUPID = 1001L;

    public static final String METRICS_CONFIG_FILE = "config.yml";

    protected final String cluster;
    protected final String namespace;
    protected final Labels labels;

    // Docker image configuration
    protected String image;
    // Number of replicas
    protected int replicas;

    protected String healthCheckPath;
    protected int healthCheckTimeout;
    protected int healthCheckInitialDelay;

    protected String headlessName;
    protected String name;

    protected final int metricsPort = 9404;
    private final String metricsPath = "/metrics";
    protected final String metricsPortName = "kafkametrics";
    protected boolean isMetricsEnabled;

    protected JsonObject metricsConfig;
    protected String metricsConfigName;

    protected Storage storage;

    protected String mountPath;
    protected String volumeName = "data";
    protected String metricsConfigVolumeName;
    protected String metricsConfigMountPath;

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where cluster resources are going to be created
     * @param cluster   overall cluster name
     */
    protected AbstractCluster(String namespace, String cluster, Labels labels) {
        this.cluster = cluster;
        this.namespace = namespace;
        this.labels = labels.withoutKind().withCluster(cluster);
    }

    public Labels getLabels() {
        return labels;
    }

    public int getReplicas() {
        return replicas;
    }

    protected void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    protected void setImage(String image) {
        this.image = image;
    }

    protected void setHealthCheckTimeout(int healthCheckTimeout) {
        this.healthCheckTimeout = healthCheckTimeout;
    }

    protected void setHealthCheckInitialDelay(int healthCheckInitialDelay) {
        this.healthCheckInitialDelay = healthCheckInitialDelay;
    }

    /**
     * Returns the Docker image which should be used by this cluster
     *
     * @return
     */
    public String getName() {
        return name;
    }

    public String getHeadlessName() {
        return headlessName;
    }

    protected Map<String, String> getLabelsWithName() {
        return getLabelsWithName(name);
    }

    protected Map<String, String> getLabelsWithName(String name) {
        return labels.withName(name).toMap();
    }

    /**
     * Returns a map with the prometheus annotations:
     * <pre><code>
     * prometheus.io/scrape: "true"
     * prometheus.io/path: "/metrics"
     * prometheus.io/port: "9404"
     * </code></pre>
     * if metrics are enabled, otherwise returns the empty map.
     */
    protected Map<String, String> getPrometheusAnnotations() {
        if (isMetricsEnabled()) {
            Map<String, String> annotations = new HashMap<>(3);
            annotations.put("prometheus.io/scrape", Boolean.toString(isMetricsEnabled()));
            annotations.put("prometheus.io/path", metricsPath);
            annotations.put("prometheus.io/port", Integer.toString(metricsPort));
            return annotations;
        } else {
            return Collections.emptyMap();
        }
    }

    public boolean isMetricsEnabled() {
        return isMetricsEnabled;
    }

    protected void setMetricsEnabled(boolean isMetricsEnabled) {
        this.isMetricsEnabled = isMetricsEnabled;
    }

    protected JsonObject getMetricsConfig() {
        return metricsConfig;
    }

    protected void setMetricsConfig(JsonObject metricsConfig) {
        this.metricsConfig = metricsConfig;
    }

    public String getMetricsConfigName() {
        return metricsConfigName;
    }

    protected void setMetricsConfigName(String metricsConfigName) {
        this.metricsConfigName = metricsConfigName;
    }

    protected List<EnvVar> getEnvVars() {
        return null;
    }

    public Storage getStorage() {
        return storage;
    }

    protected void setStorage(Storage storage) {
        this.storage = storage;
    }

    public String getVolumeName() {
        return this.volumeName;
    }

    public String getImage() {
        return this.image;
    }

    /**
     * @return the service account used by the deployed cluster for Kubernetes/OpenShift API operations
     */
    protected String getServiceAccountName() {
        return null;
    }

    /**
     * @return the cluster name
     */
    public String getCluster() {
        return cluster;
    }

    public String getPersistentVolumeClaimName(int podId) {
        return volumeName + "-" + name + "-" + podId;
    }

    public String getPodName(int podId) {
        return name + "-" + podId;
    }

    protected VolumeMount createVolumeMount(String name, String path) {
        VolumeMount volumeMount = new VolumeMountBuilder()
                .withName(name)
                .withMountPath(path)
                .build();
        log.trace("Created volume mount {}", volumeMount);
        return volumeMount;
    }

    protected ContainerPort createContainerPort(String name, int port, String protocol) {
        ContainerPort containerPort = new ContainerPortBuilder()
                .withName(name)
                .withProtocol(protocol)
                .withContainerPort(port)
                .build();
        log.trace("Created container port {}", containerPort);
        return containerPort;
    }

    protected ServicePort createServicePort(String name, int port, int targetPort, String protocol) {
        ServicePort servicePort = new ServicePortBuilder()
                .withName(name)
                .withProtocol(protocol)
                .withPort(port)
                .withNewTargetPort(targetPort)
                .build();
        log.trace("Created service port {}", servicePort);
        return servicePort;
    }

    protected PersistentVolumeClaim createPersistentVolumeClaim(String name) {

        Map<String, Quantity> requests = new HashMap<>();
        requests.put("storage", storage.size());

        PersistentVolumeClaim pvc = new PersistentVolumeClaimBuilder()
                .withNewMetadata()
                .withName(name)
                .endMetadata()
                .withNewSpec()
                .withAccessModes("ReadWriteOnce")
                .withNewResources()
                .withRequests(requests)
                .endResources()
                .withStorageClassName(storage.storageClass())
                .withSelector(storage.selector())
                .endSpec()
                .build();

        return pvc;
    }

    protected Volume createEmptyDirVolume(String name) {
        Volume volume = new VolumeBuilder()
                .withName(name)
                .withNewEmptyDir()
                .endEmptyDir()
                .build();
        log.trace("Created emptyDir volume named {}", volume);
        return volume;
    }

    protected Volume createConfigMapVolume(String name, String configMapName) {

        ConfigMapVolumeSource configMapVolumeSource = new ConfigMapVolumeSourceBuilder()
                .withName(configMapName)
                .build();

        Volume volume = new VolumeBuilder()
                .withName(name)
                .withConfigMap(configMapVolumeSource)
                .build();
        log.info("Created configMap volume {}", volume);
        return volume;
    }

    protected ConfigMap createConfigMap(String name, Map<String, String> data) {

        ConfigMap cm = new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels.toMap())
                .endMetadata()
                .withData(data)
                .build();

        return cm;
    }

    protected Probe createExecProbe(String command, int initialDelay, int timeout) {
        Probe probe = new ProbeBuilder().withNewExec()
                .withCommand(command)
                .endExec()
                .withInitialDelaySeconds(initialDelay)
                .withTimeoutSeconds(timeout)
                .build();
        log.trace("Created exec probe {}", probe);
        return probe;
    }

    protected Probe createHttpProbe(String path, String port, int initialDelay, int timeout) {
        Probe probe = new ProbeBuilder().withNewHttpGet()
                .withPath(path)
                .withNewPort(port)
                .endHttpGet()
                .withInitialDelaySeconds(initialDelay)
                .withTimeoutSeconds(timeout)
                .build();
        log.trace("Created http probe {}", probe);
        return probe;
    }

    protected Service createService(String type, List<ServicePort> ports) {
        Service service = new ServiceBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(getLabelsWithName())
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withType(type)
                    .withSelector(getLabelsWithName())
                    .withPorts(ports)
                .endSpec()
                .build();
        log.trace("Created service {}", service);
        return service;
    }

    protected Service createHeadlessService(String name, List<ServicePort> ports) {
        return createHeadlessService(name, ports, Collections.emptyMap());
    }

    protected Service createHeadlessService(String name, List<ServicePort> ports, Map<String, String> annotations) {
        Service service = new ServiceBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(getLabelsWithName(name))
                    .withNamespace(namespace)
                    .withAnnotations(annotations)
                .endMetadata()
                .withNewSpec()
                    .withType("ClusterIP")
                    .withClusterIP("None")
                    .withSelector(getLabelsWithName())
                    .withPorts(ports)
                .endSpec()
                .build();
        log.trace("Created headless service {}", service);
        return service;
    }

    protected StatefulSet createStatefulSet(
            List<ContainerPort> ports,
            List<Volume> volumes,
            List<PersistentVolumeClaim> volumeClaims,
            List<VolumeMount> volumeMounts,
            Probe livenessProbe,
            Probe readinessProbe,
            boolean isOpenShift) {

        Map<String, String> annotations = new HashMap<>();
        annotations.put(String.format("%s/%s", ClusterController.STRIMZI_CLUSTER_CONTROLLER_DOMAIN, Storage.DELETE_CLAIM_FIELD),
                String.valueOf(storage.isDeleteClaim()));

        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(getImage())
                .withEnv(getEnvVars())
                .withVolumeMounts(volumeMounts)
                .withPorts(ports)
                .withLivenessProbe(livenessProbe)
                .withReadinessProbe(readinessProbe)
                .build();

        List<Container> initContainers = new ArrayList<>();
        PodSecurityContext securityContext = null;
        // if a persistent volume claim is requested and the running cluster is a Kubernetes one
        // there is an hack on volume mounting which needs an "init-container"
        if ((this.storage.type() == Storage.StorageType.PERSISTENT_CLAIM) && !isOpenShift) {

            String chown = String.format("chown -R %d:%d %s",
                    AbstractCluster.VOLUME_MOUNT_HACK_GROUPID,
                    AbstractCluster.VOLUME_MOUNT_HACK_GROUPID,
                    volumeMounts.get(0).getMountPath());

            Container initContainer = new ContainerBuilder()
                    .withName(AbstractCluster.VOLUME_MOUNT_HACK_NAME)
                    .withImage(AbstractCluster.VOLUME_MOUNT_HACK_IMAGE)
                    .withVolumeMounts(volumeMounts.get(0))
                    .withCommand("sh", "-c", chown)
                    .build();

            initContainers.add(initContainer);

            securityContext = new PodSecurityContextBuilder()
                    .withFsGroup(AbstractCluster.VOLUME_MOUNT_HACK_GROUPID)
                    .build();
        }

        StatefulSet statefulSet = new StatefulSetBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(getLabelsWithName())
                    .withNamespace(namespace)
                    .withAnnotations(annotations)
                .endMetadata()
                .withNewSpec()
                    .withPodManagementPolicy("Parallel")
                    .withUpdateStrategy(new StatefulSetUpdateStrategyBuilder().withType("OnDelete").build())
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(getLabelsWithName()).build())
                    .withServiceName(headlessName)
                    .withReplicas(replicas)
                    .withNewTemplate()
                        .withNewMetadata()
                            .withName(name)
                            .withLabels(getLabelsWithName())
                            .withAnnotations(getPrometheusAnnotations())
                        .endMetadata()
                        .withNewSpec()
                            .withSecurityContext(securityContext)
                            .withInitContainers(initContainers)
                            .withContainers(container)
                            .withVolumes(volumes)
                        .endSpec()
                    .endTemplate()
                    .withVolumeClaimTemplates(volumeClaims)
                .endSpec()
                .build();

        return statefulSet;
    }

    protected Deployment createDeployment(
            List<ContainerPort> ports,
            Probe livenessProbe,
            Probe readinessProbe,
            DeploymentStrategy updateStrategy,
            Map<String, String> deploymentAnnotations,
            Map<String, String> podAnnotations) {

        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(getImage())
                .withEnv(getEnvVars())
                .withPorts(ports)
                .withLivenessProbe(livenessProbe)
                .withReadinessProbe(readinessProbe)
                .build();

        Deployment dep = new DeploymentBuilder()
                .withNewMetadata()
                .withName(name)
                .withLabels(getLabelsWithName())
                .withNamespace(namespace)
                .withAnnotations(deploymentAnnotations)
                .endMetadata()
                .withNewSpec()
                .withStrategy(updateStrategy)
                .withReplicas(replicas)
                .withNewTemplate()
                .withNewMetadata()
                .withLabels(getLabelsWithName())
                .withAnnotations(podAnnotations)
                .endMetadata()
                .withNewSpec()
                .withServiceAccountName(getServiceAccountName())
                .withContainers(container)
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();

        return dep;
    }

    public Service patchService(Service svc) {
        svc.getMetadata().setLabels(getLabelsWithName());
        svc.getSpec().setSelector(getLabelsWithName());
        log.trace("Patched service {}", svc);
        return svc;
    }

    protected Service patchHeadlessService(String name, Service svc) {
        svc.getMetadata().setLabels(getLabelsWithName(name));
        svc.getSpec().setSelector(getLabelsWithName());
        log.trace("Patched headless service {}", svc);
        return svc;
    }

    protected StatefulSet patchStatefulSet(StatefulSet statefulSet,
                                           Probe livenessProbe,
                                           Probe readinessProbe,
                                           Map<String, String> annotations) {

        statefulSet.getMetadata().setLabels(getLabelsWithName());
        statefulSet.getMetadata().getAnnotations().putAll(annotations);
        statefulSet.getSpec().setSelector(new LabelSelectorBuilder().withMatchLabels(getLabelsWithName()).build());
        statefulSet.getSpec().getTemplate().getMetadata().setLabels(getLabelsWithName());
        Container container = statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0);
        container.setImage(getImage());
        container.setLivenessProbe(livenessProbe);
        container.setReadinessProbe(readinessProbe);
        container.setEnv(getEnvVars());

        return statefulSet;
    }

    protected Deployment patchDeployment(Deployment dep,
                                      Probe livenessProbe,
                                      Probe readinessProbe,
                                      Map<String, String> deploymentAnnotations,
                                      Map<String, String> podAnnotations) {

        dep.getMetadata().setLabels(getLabelsWithName());
        dep.getMetadata().setAnnotations(deploymentAnnotations);
        dep.getSpec().getTemplate().getMetadata().setLabels(getLabelsWithName());
        dep.getSpec().getTemplate().getMetadata().setAnnotations(podAnnotations);
        Container container = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        container.setImage(getImage());
        container.setLivenessProbe(livenessProbe);
        container.setReadinessProbe(readinessProbe);
        container.setEnv(getEnvVars());

        return dep;
    }

    protected ConfigMap patchConfigMap(ConfigMap cm, Map<String, String> data) {

        cm.setData(data);

        return cm;
    }

    /**
     * Build an environment variable instance with the provided name and value
     *
     * @param name The name of the environment variable
     * @param value The value of the environment variable
     * @return The environment variable instance
     */
    protected static EnvVar buildEnvVar(String name, String value) {
        return new EnvVarBuilder().withName(name).withValue(value).build();
    }

    /**
     * Gets the given container's environment.
     */
    protected static Map<String, String> containerEnvVars(Container container) {
        return container.getEnv().stream().collect(
                Collectors.toMap(EnvVar::getName, EnvVar::getValue));
    }
}
