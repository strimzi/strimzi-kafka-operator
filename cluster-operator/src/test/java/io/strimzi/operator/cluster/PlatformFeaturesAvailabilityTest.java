/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;


import io.strimzi.operator.cluster.operator.KubernetesVersion;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PlatformFeaturesAvailabilityTest {

    @Test
    public void versionTest() {

        KubernetesVersion kv1p9 = new KubernetesVersion("1", "5");
        assertTrue(kv1p9.compareTo(KubernetesVersion.V1_8) < 0);
        assertTrue(kv1p9.compareTo(KubernetesVersion.V1_9) < 0);
        assertTrue(kv1p9.compareTo(KubernetesVersion.V1_10) < 0);
        assertTrue(kv1p9.compareTo(KubernetesVersion.V1_11) < 0);

        assertTrue(KubernetesVersion.V1_8.compareTo(KubernetesVersion.V1_8) == 0);
        assertTrue(KubernetesVersion.V1_8.compareTo(KubernetesVersion.V1_9) < 0);
        assertTrue(KubernetesVersion.V1_8.compareTo(KubernetesVersion.V1_10) < 0);
        assertTrue(KubernetesVersion.V1_8.compareTo(KubernetesVersion.V1_11) < 0);


        assertTrue(KubernetesVersion.V1_12.compareTo(KubernetesVersion.V1_8) > 0);
        assertTrue(KubernetesVersion.V1_12.compareTo(KubernetesVersion.V1_9) > 0);
        assertTrue(KubernetesVersion.V1_12.compareTo(KubernetesVersion.V1_10) > 0);
        assertTrue(KubernetesVersion.V1_12.compareTo(KubernetesVersion.V1_11) > 0);
        assertTrue(KubernetesVersion.V1_12.compareTo(KubernetesVersion.V1_12) == 0);

        KubernetesVersion kv2p9 = new KubernetesVersion("2", "9");
        assertTrue(kv2p9.compareTo(KubernetesVersion.V1_8) > 0);
        assertTrue(kv2p9.compareTo(KubernetesVersion.V1_9) > 0);
        assertTrue(kv2p9.compareTo(KubernetesVersion.V1_10) > 0);
        assertTrue(kv2p9.compareTo(KubernetesVersion.V1_11) > 0);
    }

    @Test
    public void versionWithCharTest() {
        KubernetesVersion kv1p9 = new KubernetesVersion("1", "9+");
        assertEquals(9, kv1p9.getMinor());
    }

    @Test
    public void networkPoliciesWithFancyCombinationTest() {
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, KubernetesVersion.V1_8);
        assertFalse(pfa.isNetworkPolicyPodSelectorAndNameSpaceInSinglePeerAvailable());
        pfa = new PlatformFeaturesAvailability(true, KubernetesVersion.V1_11);
        assertTrue(pfa.isNetworkPolicyPodSelectorAndNameSpaceInSinglePeerAvailable());
    }

    @Test
    public void versionsEqualTest() {
        KubernetesVersion kv1p9 = new KubernetesVersion("1", "9+");
        assertEquals(kv1p9, KubernetesVersion.V1_9);
    }
}
