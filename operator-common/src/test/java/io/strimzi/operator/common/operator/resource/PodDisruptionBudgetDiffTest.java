/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudgetBuilder;
import org.junit.jupiter.api.Test;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class PodDisruptionBudgetDiffTest  {
    @Test
    public void testNoDiff()    {
        PodDisruptionBudget pdb1 = new PodDisruptionBudgetBuilder()
                .withNewMetadata()
                    .withName("my-pdb")
                    .withNamespace("my-ns")
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withNewSpec()
                    .withNewMaxUnavailable(1)
                .endSpec()
                .build();

        PodDisruptionBudget pdb2 = new PodDisruptionBudgetBuilder()
                .withNewMetadata()
                    .withName("my-pdb")
                    .withNamespace("my-ns")
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withNewSpec()
                    .withNewMaxUnavailable(1)
                .endSpec()
                .build();

        assertThat((new PodDisruptionBudgetDiff(pdb1, pdb2)).isEmpty(), is(true));
    }

    @Test
    public void testMaxUnavailableDiff()    {
        PodDisruptionBudget pdb1 = new PodDisruptionBudgetBuilder()
                .withNewMetadata()
                    .withName("my-pdb")
                    .withNamespace("my-ns")
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withNewSpec()
                    .withNewMaxUnavailable(1)
                .endSpec()
                .build();

        PodDisruptionBudget pdb2 = new PodDisruptionBudgetBuilder()
                .withNewMetadata()
                    .withName("my-pdb")
                    .withNamespace("my-ns")
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withNewSpec()
                    .withNewMaxUnavailable(2)
                .endSpec()
                .build();

        assertThat((new PodDisruptionBudgetDiff(pdb1, pdb2)).isEmpty(), is(false));
    }

    @Test
    public void testMetadataDiff()    {
        PodDisruptionBudget pdb1 = new PodDisruptionBudgetBuilder()
                .withNewMetadata()
                    .withName("my-pdb")
                    .withNamespace("my-ns")
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withNewSpec()
                    .withNewMaxUnavailable(1)
                .endSpec()
                .build();

        PodDisruptionBudget pdb2 = new PodDisruptionBudgetBuilder()
                .withNewMetadata()
                    .withName("my-pdb")
                    .withNamespace("my-ns")
                    .withLabels(singletonMap("foo", "bar2"))
                .endMetadata()
                .withNewSpec()
                    .withNewMaxUnavailable(1)
                .endSpec()
                .build();

        assertThat((new PodDisruptionBudgetDiff(pdb1, pdb2)).isEmpty(), is(false));
    }
}
