package io.enmasse.barnabas.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractResource implements Resource {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected final String name;
    protected final String namespace;
    protected Map<String, String> labels = new HashMap<>();

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
        VolumeMount volumeMount = new VolumeMountBuilder()
                .withName(name)
                .withMountPath(path)
                .build();
        log.trace("Creating volume mount {}", volumeMount);
        return volumeMount;
    }

    protected ContainerPort createContainerPort(String name, int port, String protocol) {
        ContainerPort containerPort = new ContainerPortBuilder()
                .withName(name)
                .withProtocol(protocol)
                .withContainerPort(port)
                .build();
        log.trace("Creating container port {}", containerPort);
        return containerPort;
    }

    protected ServicePort createServicePort(String name, int port, int targetPort, String protocol) {
        ServicePort servicePort = new ServicePortBuilder()
                .withName(name)
                .withProtocol(protocol)
                .withPort(port)
                .withNewTargetPort(targetPort)
                .build();
        log.trace("Creating service port {}", servicePort);
        return servicePort;
    }

    protected Volume createEmptyDirVolume(String name) {
        Volume volume = new VolumeBuilder()
                .withName(name)
                .withNewEmptyDir()
                .endEmptyDir()
                .build();
        log.trace("Creating emptyDir volume named {}", volume);
        return volume;
    }

    protected Probe createExecProbe(String command, int initialDelay, int timeout) {
        Probe probe = new ProbeBuilder().withNewExec()
                .withCommand(command)
                .endExec()
                .withInitialDelaySeconds(initialDelay)
                .withTimeoutSeconds(timeout)
                .build();
        log.trace("Creating exec probe {}", probe);
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
        log.trace("Creating http probe {}", probe);
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
        log.trace("Creating service {}", service);
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
        log.trace("Creating headless service {}", service);
        return service;
    }
}
