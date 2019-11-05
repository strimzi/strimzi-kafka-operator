/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator;

import org.junit.jupiter.api.Test;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class KubernetesVersionTest {
    @Test
    public void versionTest() {
        KubernetesVersion kv1p9 = new KubernetesVersion(1, 5);
        assertThat(kv1p9.compareTo(KubernetesVersion.V1_8) < 0, is(true));
        assertThat(kv1p9.compareTo(KubernetesVersion.V1_9) < 0, is(true));
        assertThat(kv1p9.compareTo(KubernetesVersion.V1_10) < 0, is(true));
        assertThat(kv1p9.compareTo(KubernetesVersion.V1_11) < 0, is(true));

        assertThat(KubernetesVersion.V1_8.compareTo(KubernetesVersion.V1_8) == 0, is(true));
        assertThat(KubernetesVersion.V1_8.compareTo(KubernetesVersion.V1_9) < 0, is(true));
        assertThat(KubernetesVersion.V1_8.compareTo(KubernetesVersion.V1_10) < 0, is(true));
        assertThat(KubernetesVersion.V1_8.compareTo(KubernetesVersion.V1_11) < 0, is(true));


        assertThat(KubernetesVersion.V1_12.compareTo(KubernetesVersion.V1_8) > 0, is(true));
        assertThat(KubernetesVersion.V1_12.compareTo(KubernetesVersion.V1_9) > 0, is(true));
        assertThat(KubernetesVersion.V1_12.compareTo(KubernetesVersion.V1_10) > 0, is(true));
        assertThat(KubernetesVersion.V1_12.compareTo(KubernetesVersion.V1_11) > 0, is(true));
        assertThat(KubernetesVersion.V1_12.compareTo(KubernetesVersion.V1_12) == 0, is(true));

        KubernetesVersion kv2p9 = new KubernetesVersion(2, 9);
        assertThat(kv2p9.compareTo(KubernetesVersion.V1_8) > 0, is(true));
        assertThat(kv2p9.compareTo(KubernetesVersion.V1_9) > 0, is(true));
        assertThat(kv2p9.compareTo(KubernetesVersion.V1_10) > 0, is(true));
        assertThat(kv2p9.compareTo(KubernetesVersion.V1_11) > 0, is(true));
    }

    @Test
    public void versionsEqualTest() {
        KubernetesVersion kv1p9 = new KubernetesVersion(1, 9);
        assertThat(kv1p9, is(KubernetesVersion.V1_9));
    }
}
