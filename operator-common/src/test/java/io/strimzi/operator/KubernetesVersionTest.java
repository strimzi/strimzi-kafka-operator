/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class KubernetesVersionTest {
    @Test
    public void versionTest() {
        KubernetesVersion kv1p16 = new KubernetesVersion(1, 5);
        assertThat(kv1p16.compareTo(KubernetesVersion.V1_15), lessThan(0));
        assertThat(kv1p16.compareTo(KubernetesVersion.V1_16), lessThan(0));
        assertThat(kv1p16.compareTo(KubernetesVersion.V1_17), lessThan(0));
        assertThat(kv1p16.compareTo(KubernetesVersion.V1_18), lessThan(0));

        assertThat(KubernetesVersion.V1_15.compareTo(KubernetesVersion.V1_15), is(0));
        assertThat(KubernetesVersion.V1_15.compareTo(KubernetesVersion.V1_16), lessThan(0));
        assertThat(KubernetesVersion.V1_15.compareTo(KubernetesVersion.V1_17), lessThan(0));
        assertThat(KubernetesVersion.V1_15.compareTo(KubernetesVersion.V1_18), lessThan(0));


        assertThat(KubernetesVersion.V1_19.compareTo(KubernetesVersion.V1_15), greaterThan(0));
        assertThat(KubernetesVersion.V1_19.compareTo(KubernetesVersion.V1_16), greaterThan(0));
        assertThat(KubernetesVersion.V1_19.compareTo(KubernetesVersion.V1_17), greaterThan(0));
        assertThat(KubernetesVersion.V1_19.compareTo(KubernetesVersion.V1_18), greaterThan(0));
        assertThat(KubernetesVersion.V1_19.compareTo(KubernetesVersion.V1_19), is(0));

        KubernetesVersion kv2p16 = new KubernetesVersion(2, 16);
        assertThat(kv2p16.compareTo(KubernetesVersion.V1_15), greaterThan(0));
        assertThat(kv2p16.compareTo(KubernetesVersion.V1_16), greaterThan(0));
        assertThat(kv2p16.compareTo(KubernetesVersion.V1_17), greaterThan(0));
        assertThat(kv2p16.compareTo(KubernetesVersion.V1_18), greaterThan(0));
    }

    @Test
    public void versionsEqualTest() {
        KubernetesVersion kv1p16 = new KubernetesVersion(1, 16);
        assertThat(kv1p16, is(KubernetesVersion.V1_16));
    }
}
