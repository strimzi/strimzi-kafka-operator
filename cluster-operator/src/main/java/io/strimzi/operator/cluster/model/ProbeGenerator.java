/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.TlsSidecar;

import java.util.Arrays;
import java.util.List;

public class ProbeGenerator {

    private final Probe probeConfig;

    private ProbeGenerator(Probe probeConfig) {
        this.probeConfig = probeConfig;
    }

    public static ProbeGenerator of(Probe probeConfig) {
        if (probeConfig == null) {
            throw new IllegalArgumentException();
        }
        return new ProbeGenerator(probeConfig);
    }

    /**
     * probeBuilder returns a ProbeBuilder pre-configured with the supplied properties
     *
     * @return ProbeBuilder
     */
    public ProbeBuilder defaultBuilder() {
        return new ProbeBuilder()
                .withInitialDelaySeconds(probeConfig.getInitialDelaySeconds())
                .withTimeoutSeconds(probeConfig.getTimeoutSeconds())
                .withPeriodSeconds(probeConfig.getPeriodSeconds())
                .withSuccessThreshold(probeConfig.getSuccessThreshold())
                .withFailureThreshold(probeConfig.getFailureThreshold());
    }

    public io.fabric8.kubernetes.api.model.Probe httpProbe(String path, String port) {
        io.fabric8.kubernetes.api.model.Probe probe = defaultBuilder()
                .withNewHttpGet()
                    .withNewPath(path)
                    .withNewPort(port)
                .endHttpGet()
                .build();
        return probe;
    }

    public io.fabric8.kubernetes.api.model.Probe execProbe(List<String> command) {
        io.fabric8.kubernetes.api.model.Probe probe = defaultBuilder()
                .withNewExec()
                    .withCommand(command)
                .endExec()
                .build();
        return probe;
    }

    public static final io.strimzi.api.kafka.model.Probe DEFAULT_TLS_SIDECAR_PROBE = new io.strimzi.api.kafka.model.ProbeBuilder()
            .withInitialDelaySeconds(TlsSidecar.DEFAULT_HEALTHCHECK_DELAY)
            .withTimeoutSeconds(TlsSidecar.DEFAULT_HEALTHCHECK_TIMEOUT)
            .build();

    protected static io.fabric8.kubernetes.api.model.Probe tlsSidecarReadinessProbe(TlsSidecar tlsSidecar) {
        Probe tlsSidecarReadinessProbe;
        if (tlsSidecar != null && tlsSidecar.getReadinessProbe() != null) {
            tlsSidecarReadinessProbe = tlsSidecar.getReadinessProbe();
        } else {
            tlsSidecarReadinessProbe = DEFAULT_TLS_SIDECAR_PROBE;
        }
        return ProbeGenerator.of(tlsSidecarReadinessProbe)
            .execProbe(Arrays.asList("/opt/stunnel/stunnel_healthcheck.sh", "2181"));
    }

    protected static io.fabric8.kubernetes.api.model.Probe tlsSidecarLivenessProbe(TlsSidecar tlsSidecar) {
        Probe tlsSidecarLivenessProbe;
        if (tlsSidecar != null && tlsSidecar.getLivenessProbe() != null) {
            tlsSidecarLivenessProbe = tlsSidecar.getLivenessProbe();
        } else {
            tlsSidecarLivenessProbe = DEFAULT_TLS_SIDECAR_PROBE;
        }
        return ProbeGenerator.of(tlsSidecarLivenessProbe)
            .execProbe(Arrays.asList("/opt/stunnel/stunnel_healthcheck.sh", "2181"));
    }
}
