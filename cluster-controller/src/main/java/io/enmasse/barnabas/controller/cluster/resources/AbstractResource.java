package io.enmasse.barnabas.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractResource implements Resource {
    private static final Logger log = LoggerFactory.getLogger(AbstractResource.class.getName());

    protected Map<String, String> labels = new HashMap<>();

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

    protected Probe createExecProbe(String command, int initialDeleay, int timeout) {
        log.trace("Creating exec probe with command {}, initial delay {} and timeout {}", command, initialDeleay, timeout);
        return new ProbeBuilder().withNewExec()
                .withCommand(command)
                .endExec()
                .withInitialDelaySeconds(initialDeleay)
                .withTimeoutSeconds(timeout)
                .build();
    }

    protected Map<String, String> labelsWithName(String name) {
        Map<String, String> labelsWithName = new HashMap<>(labels);
        labelsWithName.put("name", name);
        return labelsWithName;
    }
}
