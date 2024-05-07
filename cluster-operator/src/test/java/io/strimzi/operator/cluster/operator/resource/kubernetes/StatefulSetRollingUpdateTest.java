/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class StatefulSetRollingUpdateTest {
    private StatefulSet currentSts;
    private StatefulSet desiredSts;

    @BeforeEach
    public void before() {
        currentSts = getStatefulSet();
        desiredSts = getStatefulSet();
    }

    private StatefulSet getStatefulSet()    {
        return new StatefulSetBuilder()
                .withNewMetadata()
                    .withName("my-sts")
                    .withNamespace("my-namespace")
                    .withLabels(Map.of("label1", "value1", "label2", "value2"))
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withNewTemplate()
                        .withNewSpec()
                            .withContainers(new ContainerBuilder()
                                    .withName("my-container")
                                    .withImage("my-image")
                                    .withNewReadinessProbe()
                                        .withInitialDelaySeconds(18)
                                        .withTimeoutSeconds(74)
                                    .endReadinessProbe()
                                    .withEnv(new EnvVar("my-env-var", "my-env-var-value", null))
                                    .build())
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }

    private StatefulSetDiff createDiff() {
        return new StatefulSetDiff(Reconciliation.DUMMY_RECONCILIATION, currentSts, desiredSts);
    }

    @Test
    public void testNotNeedsRollingUpdateWhenIdentical() {
        assertThat(StatefulSetOperator.needsRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, createDiff()), is(false));
    }

    @Test
    public void testNotNeedsRollingUpdateWhenReplicasDecrease() {
        currentSts.getSpec().setReplicas(desiredSts.getSpec().getReplicas() + 1);
        assertThat(StatefulSetOperator.needsRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, createDiff()), is(false));
    }

    @Test
    public void testNeedsRollingUpdateWhenLabelsRemoved() {
        Map<String, String> labels = new HashMap<>(desiredSts.getMetadata().getLabels());
        labels.put("foo", "bar");
        currentSts.getMetadata().setLabels(labels);
        assertThat(StatefulSetOperator.needsRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, createDiff()), is(true));
    }

    @Test
    public void testNeedsRollingUpdateWhenImageChanges() {
        String newImage = currentSts.getSpec().getTemplate().getSpec().getContainers().get(0).getImage() + "-foo";
        currentSts.getSpec().getTemplate().getSpec().getContainers().get(0).setImage(newImage);
        assertThat(StatefulSetOperator.needsRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, createDiff()), is(true));
    }

    @Test
    public void testNeedsRollingUpdateWhenReadinessDelayChanges() {
        Integer newDelay = currentSts.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds() + 1;
        currentSts.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().setInitialDelaySeconds(newDelay);
        assertThat(StatefulSetOperator.needsRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, createDiff()), is(true));
    }

    @Test
    public void testNeedsRollingUpdateWhenReadinessTimeoutChanges() {
        Integer newTimeout = currentSts.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds() + 1;
        currentSts.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().setTimeoutSeconds(newTimeout);
        assertThat(StatefulSetOperator.needsRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, createDiff()), is(true));
    }

    @Test
    public void testNeedsRollingUpdateWhenNewEnvRemoved() {
        String envVar = "SOME_RANDOM_ENV";
        currentSts.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().add(new EnvVar(envVar,
                "foo", null));
        assertThat(StatefulSetOperator.needsRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, createDiff()), is(true));
    }
}
