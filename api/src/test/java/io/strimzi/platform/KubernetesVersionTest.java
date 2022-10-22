/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.platform;

import org.junit.jupiter.api.Test;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class KubernetesVersionTest {
    @Test
    public void versionTest() {
        KubernetesVersion kv1p5 = new KubernetesVersion(1, 5);
        assertThat(kv1p5.compareTo(KubernetesVersion.V1_15), lessThan(0));
        assertThat(kv1p5.compareTo(KubernetesVersion.V1_19), lessThan(0));
        assertThat(kv1p5.compareTo(KubernetesVersion.V1_25), lessThan(0));

        assertThat(KubernetesVersion.V1_15.compareTo(KubernetesVersion.V1_15), is(0));
        assertThat(KubernetesVersion.V1_15.compareTo(KubernetesVersion.V1_19), lessThan(0));
        assertThat(KubernetesVersion.V1_15.compareTo(KubernetesVersion.V1_25), lessThan(0));


        assertThat(KubernetesVersion.V1_22.compareTo(KubernetesVersion.V1_15), greaterThan(0));
        assertThat(KubernetesVersion.V1_22.compareTo(KubernetesVersion.V1_19), greaterThan(0));
        assertThat(KubernetesVersion.V1_22.compareTo(KubernetesVersion.V1_22), is(0));

        KubernetesVersion kv2p22 = new KubernetesVersion(2, 22);
        assertThat(kv2p22.compareTo(KubernetesVersion.V1_15), greaterThan(0));
        assertThat(kv2p22.compareTo(KubernetesVersion.V1_19), greaterThan(0));
        assertThat(kv2p22.compareTo(KubernetesVersion.V1_20), greaterThan(0));
        assertThat(kv2p22.compareTo(KubernetesVersion.V1_21), greaterThan(0));
    }

    @Test
    public void versionsEqualTest() {
        KubernetesVersion kv1p25 = new KubernetesVersion(1, 25);
        assertThat(kv1p25, is(KubernetesVersion.V1_25));
    }
}
