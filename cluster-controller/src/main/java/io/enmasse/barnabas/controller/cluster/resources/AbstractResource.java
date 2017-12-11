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

    protected final String name;
    protected final String namespace;
    protected Map<String, String> labels = new HashMap<>();

    protected final int LOCK_TIMEOUT = 60000;

    protected AbstractResource(String namespace, String name) {
        this.name = name;
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

    protected VolumeMount createVolumeMount(String name, String path) {
        log.trace("Creating volume mount {} with path {}", name, path);
        return new VolumeMountBuilder()
                .withName(name)
                .withMountPath(path)
                .build();
    }

    protected ContainerPort createContainerPort(String name, int port) {
        log.trace("Creating container port {} named {}", port, name);
        return new ContainerPortBuilder()
                .withName(name)
                .withProtocol("TCP")
                .withContainerPort(port)
                .build();
    }

    protected ServicePort createServicePort(String name, int port, int targetPort) {
        log.trace("Creating service port {} with target port {} named {}", port, targetPort, name);
        return new ServicePortBuilder()
                .withName(name)
                .withProtocol("TCP")
                .withPort(port)
                .withNewTargetPort(targetPort)
                .build();
    }

    protected Volume createEmptyDirVolume(String name) {
        log.trace("Creating emptyDir volume named {}", name);
        return new VolumeBuilder()
                .withName(name)
                .withNewEmptyDir()
                .endEmptyDir()
                .build();
    }

    protected Probe createExecProbe(String command, int initialDelay, int timeout) {
        log.trace("Creating exec probe with command {}, initial delay {} and timeout {}", command, initialDelay, timeout);
        return new ProbeBuilder().withNewExec()
                .withCommand(command)
                .endExec()
                .withInitialDelaySeconds(initialDelay)
                .withTimeoutSeconds(timeout)
                .build();
    }

    protected Probe createHttpProbe(String path, String port, int initialDelay, int timeout) {
        log.trace("Creating http probe with path {}, port {}, initial delay {} and timeout {}", path, port, initialDelay, timeout);
        return new ProbeBuilder().withNewHttpGet()
                .withPath(path)
                .withNewPort(port)
                .endHttpGet()
                .withInitialDelaySeconds(initialDelay)
                .withTimeoutSeconds(timeout)
                .build();
    }
}
