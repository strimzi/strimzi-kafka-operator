/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ParallelSuite
public class FeatureGatesTest {
    @ParallelTest
    public void testFeatureGates() {
        assertThat(new FeatureGates("+ControlPlaneListener").controlPlaneListenerEnabled(), is(true));
        assertThat(new FeatureGates("+ServiceAccountPatching").serviceAccountPatchingEnabled(), is(true));
        assertThat(new FeatureGates("+ControlPlaneListener,-ServiceAccountPatching").controlPlaneListenerEnabled(), is(true));
        assertThat(new FeatureGates("+ControlPlaneListener,-ServiceAccountPatching").serviceAccountPatchingEnabled(), is(false));
        assertThat(new FeatureGates("  +ControlPlaneListener    ,    +ServiceAccountPatching").controlPlaneListenerEnabled(), is(true));
        assertThat(new FeatureGates("  +ControlPlaneListener    ,    +ServiceAccountPatching").serviceAccountPatchingEnabled(), is(true));
        assertThat(new FeatureGates("+ServiceAccountPatching,-ControlPlaneListener").controlPlaneListenerEnabled(), is(false));
        assertThat(new FeatureGates("+ServiceAccountPatching,-ControlPlaneListener").serviceAccountPatchingEnabled(), is(true));
    }

    @ParallelTest
    public void testEmptyFeatureGates() {
        assertThat(new FeatureGates(null).controlPlaneListenerEnabled(), is(false));
        assertThat(new FeatureGates("").controlPlaneListenerEnabled(), is(false));
        assertThat(new FeatureGates(" ").controlPlaneListenerEnabled(), is(false));
        assertThat(new FeatureGates("    ").controlPlaneListenerEnabled(), is(false));
    }

    @ParallelTest
    public void testDuplicateFeatureGateWithSameValue() {
        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> new FeatureGates("+ControlPlaneListener,+ControlPlaneListener"));
        assertThat(e.getMessage(), containsString("Feature gate ControlPlaneListener is configured multiple times"));
    }

    @ParallelTest
    public void testDuplicateFeatureGateWithDifferentValue() {
        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> new FeatureGates("+ControlPlaneListener,-ControlPlaneListener"));
        assertThat(e.getMessage(), containsString("Feature gate ControlPlaneListener is configured multiple times"));
    }

    @ParallelTest
    public void testMissingSign() {
        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> new FeatureGates("ControlPlaneListener"));
        assertThat(e.getMessage(), containsString("ControlPlaneListener is not a valid feature gate configuration"));
    }

    @ParallelTest
    public void testNonExistingGate() {
        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> new FeatureGates("+RandomGate"));
        assertThat(e.getMessage(), containsString("Unknown feature gate RandomGate found in the configuration"));
    }
}
