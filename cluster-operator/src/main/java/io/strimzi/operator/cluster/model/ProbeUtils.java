/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.strimzi.api.kafka.model.common.HasLivenessProbe;
import io.strimzi.api.kafka.model.common.HasReadinessProbe;
import io.strimzi.api.kafka.model.common.HasStartupProbe;
import io.strimzi.api.kafka.model.common.Probe;

import java.util.List;

/**
 * Utility for generating healthcheck probes
 */
public class ProbeUtils {
    /**
     * Default healthcheck options used by most of our operands with the exception of Mirror Maker (1 and 2), Connect,
     * and User / Topic operators.
     */
    public static final Probe DEFAULT_HEALTHCHECK_OPTIONS = new io.strimzi.api.kafka.model.common.ProbeBuilder().withTimeoutSeconds(5).withInitialDelaySeconds(15).build();

    private ProbeUtils() { }

    /**
     * probeBuilder returns a ProbeBuilder pre-configured with the supplied properties
     *
     * @param probeConfig the initial config for the ProbeBuilder
     * @return ProbeBuilder
     */
    public static ProbeBuilder defaultBuilder(Probe probeConfig) {
        if (probeConfig == null) {
            throw new IllegalArgumentException();
        }
        
        ProbeBuilder pb =  new ProbeBuilder()
                .withTimeoutSeconds(probeConfig.getTimeoutSeconds())
                .withPeriodSeconds(probeConfig.getPeriodSeconds())
                .withSuccessThreshold(probeConfig.getSuccessThreshold())
                .withFailureThreshold(probeConfig.getFailureThreshold());

        if (probeConfig.getInitialDelaySeconds() > 0)   {
            pb = pb.withInitialDelaySeconds(probeConfig.getInitialDelaySeconds());
        }

        return pb;
    }

    /**
     * Creates HTTP based probe
     *
     * @param probeConfig   Probe configuration
     * @param path          Path which should be checked
     * @param port          Port which should be used
     *
     * @return  Kubernetes Probe
     */
    public static io.fabric8.kubernetes.api.model.Probe httpProbe(Probe probeConfig, String path, String port) {
        if (path == null || path.isEmpty() || port == null || port.isEmpty()) {
            throw new IllegalArgumentException();
        }

        return defaultBuilder(probeConfig)
                .withNewHttpGet()
                    .withPath(path)
                    .withNewPort(port)
                .endHttpGet()
                .build();
    }

    /**
     * Creates Exec based probe
     *
     * @param probeConfig   Probe configuration
     * @param command       Command which should be executed
     *
     * @return  Kubernetes Probe
     */
    public static io.fabric8.kubernetes.api.model.Probe execProbe(Probe probeConfig, List<String> command) {
        if (command == null || command.isEmpty()) {
            throw new IllegalArgumentException();
        }

        return defaultBuilder(probeConfig)
                .withNewExec()
                    .withCommand(command)
                .endExec()
                .build();
    }

    /**
     * Extracts custom configuration of liveness probe from the custom resource or uses the default value.
     *
     * @param spec              The custom resource which might have the liveness probe configuration
     * @param defaultOptions    The default configuration which will be used when the liveness probe options in the
     *                          custom resource are not set
     *
     * @return  Probe options configured by the user or the defaults
     */
    public static Probe extractLivenessProbeOptionsOrDefault(HasLivenessProbe spec, Probe defaultOptions)    {
        if (spec.getLivenessProbe() != null)    {
            return spec.getLivenessProbe();
        } else {
            return defaultOptions;
        }
    }

    /**
     * Extracts custom configuration of readiness probe from the custom resource or uses the default value.
     *
     * @param spec              The custom resource which might have the readiness probe configuration
     * @param defaultOptions    The default configuration which will be used when the readiness probe options in the
     *                          custom resource are not set
     *
     * @return  Probe options configured by the user or the defaults
     */
    public static Probe extractReadinessProbeOptionsOrDefault(HasReadinessProbe spec, Probe defaultOptions)    {
        if (spec.getReadinessProbe() != null)    {
            return spec.getReadinessProbe();
        } else {
            return defaultOptions;
        }
    }

    /**
     * Extracts custom configuration of startup probe from the custom resource or uses the default value.
     *
     * @param spec              The custom resource which might have the startup probe configuration
     * @param defaultOptions    The default configuration which will be used when the liveness probe options in the
     *                          custom resource are not set
     *
     * @return  Probe options configured by the user or the defaults
     */
    public static Probe extractStartupProbeOptionsOrDefault(HasStartupProbe spec, Probe defaultOptions)    {
        if (spec.getStartupProbe() != null)    {
            return spec.getStartupProbe();
        } else {
            return defaultOptions;
        }
    }
}
