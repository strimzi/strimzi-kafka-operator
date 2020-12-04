/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.strimzi.api.kafka.model.TlsSidecar;
import io.strimzi.api.kafka.model.TlsSidecarBuilder;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProbeGeneratorTest {

    private static final io.strimzi.api.kafka.model.Probe DEFAULT_CONFIG = new io.strimzi.api.kafka.model.ProbeBuilder()
            .withInitialDelaySeconds(1)
            .withTimeoutSeconds(2)
            .withPeriodSeconds(3)
            .withSuccessThreshold(4)
            .withFailureThreshold(5)
            .build();

    @Test
    public void testNullProbeConfigThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> ProbeGenerator.defaultBuilder(null));
    }

    @Test
    public void testDefaultBuilder() {
        Probe probe = ProbeGenerator.defaultBuilder(DEFAULT_CONFIG)
                .build();
        assertThat(probe, is(new ProbeBuilder()
                .withInitialDelaySeconds(1)
                .withTimeoutSeconds(2)
                .withPeriodSeconds(3)
                .withSuccessThreshold(4)
                .withFailureThreshold(5)
                .build()
        ));

    }

    // Inherits defaults from the io.strimzi.api.kafka.model.Probe class
    @Test
    public void testDefaultBuilderNoValues() {
        Probe probe = ProbeGenerator.defaultBuilder(new io.strimzi.api.kafka.model.ProbeBuilder().build())
                .build();
        assertThat(probe, is(new ProbeBuilder()
                .withInitialDelaySeconds(15)
                .withTimeoutSeconds(5)
                .build()
        ));
    }

    @Test
    public void testHttpProbe() {
        Probe probe = ProbeGenerator.httpProbe(DEFAULT_CONFIG, "path", "1001");
        assertThat(probe, is(new ProbeBuilder()
                .withNewHttpGet()
                    .withNewPath("path")
                    .withNewPort("1001")
                .endHttpGet()
                .withInitialDelaySeconds(1)
                .withTimeoutSeconds(2)
                .withPeriodSeconds(3)
                .withSuccessThreshold(4)
                .withFailureThreshold(5)
                .build()
        ));
    }

    @Test
    public void testHttpProbeMissingPathThrows() {
        assertThrows(IllegalArgumentException.class, () -> ProbeGenerator.httpProbe(DEFAULT_CONFIG, null, "1001"));
        assertThrows(IllegalArgumentException.class, () -> ProbeGenerator.httpProbe(DEFAULT_CONFIG, "", "1001"));
    }

    @Test
    public void testHttpProbeMissingPortThrows() {
        assertThrows(IllegalArgumentException.class, () -> ProbeGenerator.httpProbe(DEFAULT_CONFIG, "path", null));
        assertThrows(IllegalArgumentException.class, () -> ProbeGenerator.httpProbe(DEFAULT_CONFIG, "path", ""));
    }

    @Test
    public void testExecProbe() {
        Probe probe = ProbeGenerator.execProbe(DEFAULT_CONFIG, Arrays.asList("command1", "command2"));
        assertThat(probe, is(new ProbeBuilder()
                .withNewExec()
                    .addToCommand("command1", "command2")
                .endExec()
                .withInitialDelaySeconds(1)
                .withTimeoutSeconds(2)
                .withPeriodSeconds(3)
                .withSuccessThreshold(4)
                .withFailureThreshold(5)
                .build()
        ));
    }

    @Test
    public void testExecProbeMissingCommandsThrows() {
        assertThrows(IllegalArgumentException.class, () -> ProbeGenerator.execProbe(DEFAULT_CONFIG, null));
        assertThrows(IllegalArgumentException.class, () -> ProbeGenerator.execProbe(DEFAULT_CONFIG, Collections.emptyList()));
    }

    private static final TlsSidecar TLS_SIDECAR = new TlsSidecarBuilder()
            .withNewLivenessProbe()
                .withInitialDelaySeconds(1)
                .withTimeoutSeconds(2)
                .withPeriodSeconds(3)
                .withSuccessThreshold(4)
                .withFailureThreshold(5)
            .endLivenessProbe()
            .withNewReadinessProbe()
                .withInitialDelaySeconds(6)
                .withTimeoutSeconds(7)
                .withPeriodSeconds(8)
                .withSuccessThreshold(9)
                .withFailureThreshold(10)
            .endReadinessProbe()
            .build();

    @Test
    public void testTlsSidecarLivenessProbe() {
        Probe probe = ProbeGenerator.tlsSidecarLivenessProbe(TLS_SIDECAR);
        assertThat(probe, is(new ProbeBuilder()
                .withNewExec()
                    .addToCommand("/opt/stunnel/stunnel_healthcheck.sh", "2181")
                .endExec()
                .withInitialDelaySeconds(1)
                .withTimeoutSeconds(2)
                .withPeriodSeconds(3)
                .withSuccessThreshold(4)
                .withFailureThreshold(5)
                .build()
        ));
    }

    @Test
    public void testTlsSidecarLivenessProbeWithNullTlsSidecarDefaults() {
        Probe probe = ProbeGenerator.tlsSidecarLivenessProbe(null);
        assertThat(probe, is(new ProbeBuilder()
                .withNewExec()
                .addToCommand("/opt/stunnel/stunnel_healthcheck.sh", "2181")
                .endExec()
                .withInitialDelaySeconds(15)
                .withTimeoutSeconds(5)
                .build()
        ));
    }

    @Test
    public void testTlsSidecarReadinessProbe() {
        Probe probe = ProbeGenerator.tlsSidecarReadinessProbe(TLS_SIDECAR);
        assertThat(probe, is(new ProbeBuilder()
                .withNewExec()
                    .addToCommand("/opt/stunnel/stunnel_healthcheck.sh", "2181")
                .endExec()
                .withInitialDelaySeconds(6)
                .withTimeoutSeconds(7)
                .withPeriodSeconds(8)
                .withSuccessThreshold(9)
                .withFailureThreshold(10)
                .build()
        ));
    }

    @Test
    public void testTlsSidecarReadinessProbeWithNullTlsSidecarDefaults() {
        Probe probe = ProbeGenerator.tlsSidecarLivenessProbe(null);
        assertThat(probe, is(new ProbeBuilder()
                .withNewExec()
                    .addToCommand("/opt/stunnel/stunnel_healthcheck.sh", "2181")
                .endExec()
                .withInitialDelaySeconds(15)
                .withTimeoutSeconds(5)
                .build()
        ));
    }

    @Test
    public void testZeroInitialDelayIsSetToNull() {
        io.strimzi.api.kafka.model.Probe probeConfig = new io.strimzi.api.kafka.model.ProbeBuilder()
                .withInitialDelaySeconds(0)
                .build();

        Probe probe = ProbeGenerator.defaultBuilder(probeConfig)
                .build();

        assertThat(probe.getInitialDelaySeconds(), is(nullValue()));
    }
}