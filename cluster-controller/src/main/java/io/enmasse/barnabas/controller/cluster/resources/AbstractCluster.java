package io.enmasse.barnabas.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.extensions.DeploymentStrategyBuilder;
import io.fabric8.kubernetes.api.model.extensions.RollingUpdateDeploymentBuilder;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetUpdateStrategyBuilder;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractCluster {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected final String name;
    protected final String namespace;
    protected Map<String, String> labels = new HashMap<>();

    // Docker image configuration
    protected String image;
    // Number of replicas
    protected int replicas;

    protected String healthCheckPath;
    protected int healthCheckTimeout;
    protected int healthCheckInitialDelay;

    protected String headlessName;

    protected final int metricsPort = 9404;
    protected final String metricsPortName = "kafkametrics";
    protected boolean isMetricsEnabled;

    protected JsonObject metricsConfig;

    protected AbstractCluster(String namespace, String name) {
        this.name = name;
        this.namespace = namespace;
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    protected void setLabels(Map<String, String> newLabels) {
        newLabels.put("cluster-name", name);
        this.labels = new HashMap<>(newLabels);
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

    public String getHeadlessName() {
        return headlessName;
    }

    protected Map<String, String> getLabelsWithName() {
        return getLabelsWithName(name);
    }

    protected Map<String, String> getLabelsWithName(String name) {
        Map<String, String> labelsWithName = new HashMap<>(labels);
        labelsWithName.put("name", name);
        return labelsWithName;
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

    protected List<EnvVar> getEnvVars() {
        return null;
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

    protected Volume createEmptyDirVolume(String name) {
        Volume volume = new VolumeBuilder()
                .withName(name)
                .withNewEmptyDir()
                .endEmptyDir()
                .build();
        log.trace("Created emptyDir volume named {}", volume);
        return volume;
    }

    protected Volume createConfigMapVolume(String name) {

        ConfigMapVolumeSource configMapVolumeSource = new ConfigMapVolumeSourceBuilder()
                .withName(name)
                .build();

        Volume volume = new VolumeBuilder()
                .withName(name)
                .withConfigMap(configMapVolumeSource)
                .build();
        log.info("Created configMap volume {}", volume);
        return volume;
    }

    protected ConfigMap createConfigMap(String name, Map<String, String> data) {

        ConfigMap metricsCm = new ConfigMapBuilder()
                .withNewMetadata()
                .withName(name)
                .withNamespace(namespace)
                .endMetadata()
                .withData(data)
                .build();

        return metricsCm;
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
        Service service = new ServiceBuilder()
                .withNewMetadata()
                .withName(name)
                .withLabels(getLabelsWithName(name))
                .withNamespace(namespace)
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
            List<VolumeMount> volumeMounts,
            Probe livenessProbe,
            Probe readinessProbe) {

        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(image)
                .withEnv(getEnvVars())
                .withVolumeMounts(volumeMounts)
                .withPorts(ports)
                .withLivenessProbe(livenessProbe)
                .withReadinessProbe(readinessProbe)
                .build();

        StatefulSet statefulSet = new StatefulSetBuilder()
                .withNewMetadata()
                .withName(name)
                .withLabels(getLabelsWithName())
                .withNamespace(namespace)
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
                .endMetadata()
                .withNewSpec()
                .withContainers(container)
                .withVolumes(volumes)
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();

        return statefulSet;
    }

    protected Deployment createDeployment(
            List<ContainerPort> ports,
            Probe livenessProbe,
            Probe readinessProbe) {

        Container container = new ContainerBuilder()
                .withName(name)
                .withImage(image)
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
                .endMetadata()
                .withNewSpec()
                .withStrategy(new DeploymentStrategyBuilder().withType("RollingUpdate").withRollingUpdate(new RollingUpdateDeploymentBuilder().withMaxSurge(new IntOrString(1)).withMaxUnavailable(new IntOrString(0)).build()).build())
                .withReplicas(replicas)
                .withNewTemplate()
                .withNewMetadata()
                .withLabels(getLabels())
                .endMetadata()
                .withNewSpec()
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
                                        Probe readinessProbe) {

        statefulSet.getMetadata().setLabels(getLabelsWithName());
        statefulSet.getSpec().setSelector(new LabelSelectorBuilder().withMatchLabels(getLabelsWithName()).build());
        statefulSet.getSpec().getTemplate().getMetadata().setLabels(getLabelsWithName());
        statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).setImage(image);
        statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).setLivenessProbe(livenessProbe);
        statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).setReadinessProbe(readinessProbe);
        statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0).setEnv(getEnvVars());

        return statefulSet;
    }

    protected Deployment patchDeployment(Deployment dep,
                                      Probe livenessProbe,
                                      Probe readinessProbe) {

        dep.getMetadata().setLabels(getLabelsWithName());
        dep.getSpec().getTemplate().getMetadata().setLabels(getLabelsWithName());
        dep.getSpec().getTemplate().getSpec().getContainers().get(0).setImage(image);
        dep.getSpec().getTemplate().getSpec().getContainers().get(0).setLivenessProbe(livenessProbe);
        dep.getSpec().getTemplate().getSpec().getContainers().get(0).setReadinessProbe(readinessProbe);
        dep.getSpec().getTemplate().getSpec().getContainers().get(0).setEnv(getEnvVars());

        return dep;
    }
}
