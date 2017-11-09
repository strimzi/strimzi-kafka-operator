package io.enmasse.barnabas.controller.cluster.model;

import io.fabric8.kubernetes.api.model.*;

public abstract class AbstractResource implements Resource {
    protected VolumeMount createVolumeMount(String name, String path) {
        return new VolumeMountBuilder()
                .withName(name)
                .withMountPath(path)
                .build();
    }

    protected ContainerPort createContainerPort(String name, int port) {
        return new ContainerPortBuilder()
                .withName(name)
                .withProtocol("TCP")
                .withContainerPort(port)
                .build();
    }

    protected ServicePort createServicePort(String name, int port, int targetPort) {
        return new ServicePortBuilder()
                .withName(name)
                .withProtocol("TCP")
                .withPort(port)
                .withNewTargetPort(targetPort)
                .build();
    }

    protected Volume createEmptyDirVolume(String name) {
        return new VolumeBuilder()
                .withName(name)
                .withNewEmptyDir()
                .endEmptyDir()
                .build();
    }

    protected Probe createExecProbe(String command, int initialDeleay, int timeout) {
        return new ProbeBuilder().withNewExec()
                .withCommand(command)
                .endExec()
                .withInitialDelaySeconds(initialDeleay)
                .withTimeoutSeconds(timeout)
                .build();
    }
}
