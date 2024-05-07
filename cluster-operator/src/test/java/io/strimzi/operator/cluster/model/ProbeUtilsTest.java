/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityTopicOperatorSpec;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityTopicOperatorSpecBuilder;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ParallelSuite
public class ProbeUtilsTest {
    private static final io.strimzi.api.kafka.model.common.Probe DEFAULT_CONFIG = new io.strimzi.api.kafka.model.common.ProbeBuilder()
            .withInitialDelaySeconds(1)
            .withTimeoutSeconds(2)
            .withPeriodSeconds(3)
            .withSuccessThreshold(4)
            .withFailureThreshold(5)
            .build();

    @ParallelTest
    public void testNullProbeConfigThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> ProbeUtils.defaultBuilder(null));
    }

    @ParallelTest
    public void testDefaultBuilder() {
        Probe probe = ProbeUtils.defaultBuilder(DEFAULT_CONFIG)
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

    // Inherits defaults from the io.strimzi.api.kafka.model.common.Probe class
    @ParallelTest
    public void testDefaultBuilderNoValues() {
        Probe probe = ProbeUtils.defaultBuilder(new io.strimzi.api.kafka.model.common.ProbeBuilder().build())
                .build();
        assertThat(probe, is(new ProbeBuilder()
                .withInitialDelaySeconds(15)
                .withTimeoutSeconds(5)
                .build()
        ));
    }

    @ParallelTest
    public void testHttpProbe() {
        Probe probe = ProbeUtils.httpProbe(DEFAULT_CONFIG, "path", "1001");
        assertThat(probe, is(new ProbeBuilder()
                .withNewHttpGet()
                    .withPath("path")
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

    @ParallelTest
    public void testHttpProbeMissingPathThrows() {
        assertThrows(IllegalArgumentException.class, () -> ProbeUtils.httpProbe(DEFAULT_CONFIG, null, "1001"));
        assertThrows(IllegalArgumentException.class, () -> ProbeUtils.httpProbe(DEFAULT_CONFIG, "", "1001"));
    }

    @ParallelTest
    public void testHttpProbeMissingPortThrows() {
        assertThrows(IllegalArgumentException.class, () -> ProbeUtils.httpProbe(DEFAULT_CONFIG, "path", null));
        assertThrows(IllegalArgumentException.class, () -> ProbeUtils.httpProbe(DEFAULT_CONFIG, "path", ""));
    }

    @ParallelTest
    public void testExecProbe() {
        Probe probe = ProbeUtils.execProbe(DEFAULT_CONFIG, Arrays.asList("command1", "command2"));
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

    @ParallelTest
    public void testExecProbeMissingCommandsThrows() {
        assertThrows(IllegalArgumentException.class, () -> ProbeUtils.execProbe(DEFAULT_CONFIG, null));
        assertThrows(IllegalArgumentException.class, () -> ProbeUtils.execProbe(DEFAULT_CONFIG, Collections.emptyList()));
    }

    @ParallelTest
    public void testZeroInitialDelayIsSetToNull() {
        io.strimzi.api.kafka.model.common.Probe probeConfig = new io.strimzi.api.kafka.model.common.ProbeBuilder()
                .withInitialDelaySeconds(0)
                .build();

        Probe probe = ProbeUtils.defaultBuilder(probeConfig)
                .build();

        assertThat(probe.getInitialDelaySeconds(), is(nullValue()));
    }

    @ParallelTest
    public void testExtractProbeOptionSet()  {
        EntityTopicOperatorSpec spec = new EntityTopicOperatorSpecBuilder()
                .withNewLivenessProbe()
                    .withInitialDelaySeconds(11)
                    .withTimeoutSeconds(12)
                    .withPeriodSeconds(13)
                    .withFailureThreshold(14)
                    .withSuccessThreshold(15)
                .endLivenessProbe()
                .withNewReadinessProbe()
                    .withInitialDelaySeconds(21)
                    .withTimeoutSeconds(22)
                    .withPeriodSeconds(23)
                    .withFailureThreshold(24)
                    .withSuccessThreshold(25)
                .endReadinessProbe()
                .withNewStartupProbe()
                    .withInitialDelaySeconds(31)
                    .withTimeoutSeconds(32)
                    .withPeriodSeconds(33)
                    .withFailureThreshold(34)
                    .withSuccessThreshold(35)
                .endStartupProbe()
                .build();

        io.strimzi.api.kafka.model.common.Probe probe = ProbeUtils.extractLivenessProbeOptionsOrDefault(spec, ProbeUtils.DEFAULT_HEALTHCHECK_OPTIONS);
        assertThat(probe.getInitialDelaySeconds(), is(11));
        assertThat(probe.getTimeoutSeconds(), is(12));
        assertThat(probe.getPeriodSeconds(), is(13));
        assertThat(probe.getFailureThreshold(), is(14));
        assertThat(probe.getSuccessThreshold(), is(15));

        probe = ProbeUtils.extractReadinessProbeOptionsOrDefault(spec, ProbeUtils.DEFAULT_HEALTHCHECK_OPTIONS);
        assertThat(probe.getInitialDelaySeconds(), is(21));
        assertThat(probe.getTimeoutSeconds(), is(22));
        assertThat(probe.getPeriodSeconds(), is(23));
        assertThat(probe.getFailureThreshold(), is(24));
        assertThat(probe.getSuccessThreshold(), is(25));

        probe = ProbeUtils.extractStartupProbeOptionsOrDefault(spec, ProbeUtils.DEFAULT_HEALTHCHECK_OPTIONS);
        assertThat(probe.getInitialDelaySeconds(), is(31));
        assertThat(probe.getTimeoutSeconds(), is(32));
        assertThat(probe.getPeriodSeconds(), is(33));
        assertThat(probe.getFailureThreshold(), is(34));
        assertThat(probe.getSuccessThreshold(), is(35));
    }

    @ParallelTest
    public void testExtractProbeOptionNotSet()  {
        EntityTopicOperatorSpec spec = new EntityTopicOperatorSpecBuilder().build();

        io.strimzi.api.kafka.model.common.Probe probe = ProbeUtils.extractLivenessProbeOptionsOrDefault(spec, ProbeUtils.DEFAULT_HEALTHCHECK_OPTIONS);
        assertThat(probe.getInitialDelaySeconds(), is(15));
        assertThat(probe.getTimeoutSeconds(), is(5));
        assertThat(probe.getPeriodSeconds(), is(nullValue()));
        assertThat(probe.getFailureThreshold(), is(nullValue()));
        assertThat(probe.getSuccessThreshold(), is(nullValue()));

        probe = ProbeUtils.extractReadinessProbeOptionsOrDefault(spec, ProbeUtils.DEFAULT_HEALTHCHECK_OPTIONS);
        assertThat(probe.getInitialDelaySeconds(), is(15));
        assertThat(probe.getTimeoutSeconds(), is(5));
        assertThat(probe.getPeriodSeconds(), is(nullValue()));
        assertThat(probe.getFailureThreshold(), is(nullValue()));
        assertThat(probe.getSuccessThreshold(), is(nullValue()));

        probe = ProbeUtils.extractStartupProbeOptionsOrDefault(spec, ProbeUtils.DEFAULT_HEALTHCHECK_OPTIONS);
        assertThat(probe.getInitialDelaySeconds(), is(15));
        assertThat(probe.getTimeoutSeconds(), is(5));
        assertThat(probe.getPeriodSeconds(), is(nullValue()));
        assertThat(probe.getFailureThreshold(), is(nullValue()));
        assertThat(probe.getSuccessThreshold(), is(nullValue()));
    }
}