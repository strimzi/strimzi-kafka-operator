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
    // Kubernetes versions used for tests => comparing Kubernetes versions
    private static final KubernetesVersion V1_5 = new KubernetesVersion(1, 5); // very old version
    private static final KubernetesVersion V1_14 = new KubernetesVersion(1, 14);
    private static final KubernetesVersion V1_15 = new KubernetesVersion(1, 15);
    private static final KubernetesVersion V1_16 = new KubernetesVersion(1, 16);

    @Test
    public void versionTest() {
        KubernetesVersion kv1p5 = new KubernetesVersion(1, 5);
        assertThat(kv1p5.compareTo(V1_14), lessThan(0));
        assertThat(kv1p5.compareTo(V1_15), lessThan(0));
        assertThat(kv1p5.compareTo(V1_16), lessThan(0));

        assertThat(V1_5.compareTo(V1_5), is(0));
        assertThat(V1_5.compareTo(V1_14), lessThan(0));
        assertThat(V1_5.compareTo(V1_15), lessThan(0));


        assertThat(V1_15.compareTo(V1_5), greaterThan(0));
        assertThat(V1_15.compareTo(V1_14), greaterThan(0));
        assertThat(V1_15.compareTo(V1_15), is(0));

        KubernetesVersion kv2p22 = new KubernetesVersion(2, 22);
        assertThat(kv2p22.compareTo(V1_5), greaterThan(0));
        assertThat(kv2p22.compareTo(V1_15), greaterThan(0));
    }

    @Test
    public void versionsEqualTest() {
        KubernetesVersion kv1p25 = new KubernetesVersion(1, 15);
        assertThat(kv1p25, is(V1_15));
    }
}
