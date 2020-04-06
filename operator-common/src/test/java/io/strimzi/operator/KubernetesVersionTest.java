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
        KubernetesVersion kv1p9 = new KubernetesVersion(1, 5);
        assertThat(kv1p9.compareTo(KubernetesVersion.V1_8), lessThan(0));
        assertThat(kv1p9.compareTo(KubernetesVersion.V1_9), lessThan(0));
        assertThat(kv1p9.compareTo(KubernetesVersion.V1_10), lessThan(0));
        assertThat(kv1p9.compareTo(KubernetesVersion.V1_11), lessThan(0));

        assertThat(KubernetesVersion.V1_8.compareTo(KubernetesVersion.V1_8), is(0));
        assertThat(KubernetesVersion.V1_8.compareTo(KubernetesVersion.V1_9), lessThan(0));
        assertThat(KubernetesVersion.V1_8.compareTo(KubernetesVersion.V1_10), lessThan(0));
        assertThat(KubernetesVersion.V1_8.compareTo(KubernetesVersion.V1_11), lessThan(0));


        assertThat(KubernetesVersion.V1_12.compareTo(KubernetesVersion.V1_8), greaterThan(0));
        assertThat(KubernetesVersion.V1_12.compareTo(KubernetesVersion.V1_9), greaterThan(0));
        assertThat(KubernetesVersion.V1_12.compareTo(KubernetesVersion.V1_10), greaterThan(0));
        assertThat(KubernetesVersion.V1_12.compareTo(KubernetesVersion.V1_11), greaterThan(0));
        assertThat(KubernetesVersion.V1_12.compareTo(KubernetesVersion.V1_12), is(0));

        KubernetesVersion kv2p9 = new KubernetesVersion(2, 9);
        assertThat(kv2p9.compareTo(KubernetesVersion.V1_8), greaterThan(0));
        assertThat(kv2p9.compareTo(KubernetesVersion.V1_9), greaterThan(0));
        assertThat(kv2p9.compareTo(KubernetesVersion.V1_10), greaterThan(0));
        assertThat(kv2p9.compareTo(KubernetesVersion.V1_11), greaterThan(0));
    }

    @Test
    public void versionsEqualTest() {
        KubernetesVersion kv1p9 = new KubernetesVersion(1, 9);
        assertThat(kv1p9, is(KubernetesVersion.V1_9));
    }
}
