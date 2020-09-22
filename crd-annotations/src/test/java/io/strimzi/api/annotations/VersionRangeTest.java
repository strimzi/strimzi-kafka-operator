/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.annotations;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class VersionRangeTest {
    @Test
    public void testContains() {

        assertTrue(ApiVersion.parseRange("v1").contains(ApiVersion.parse("v1")));
        assertFalse(ApiVersion.parseRange("v1").contains(ApiVersion.parse("v1alpha1")));
        assertFalse(ApiVersion.parseRange("v1").contains(ApiVersion.parse("v1beta1")));
        assertFalse(ApiVersion.parseRange("v1").contains(ApiVersion.parse("v2")));

        assertTrue(ApiVersion.parseRange("v1alpha1-v1").contains(ApiVersion.parse("v1alpha1")));
        assertTrue(ApiVersion.parseRange("v1alpha1-v1").contains(ApiVersion.parse("v1alpha2")));
        assertTrue(ApiVersion.parseRange("v1alpha1-v1").contains(ApiVersion.parse("v1beta1")));
        assertTrue(ApiVersion.parseRange("v1alpha1-v1").contains(ApiVersion.parse("v1beta2")));
        assertTrue(ApiVersion.parseRange("v1alpha1-v1").contains(ApiVersion.parse("v1")));
        assertFalse(ApiVersion.parseRange("v1alpha1-v1").contains(ApiVersion.parse("v0")));
        assertFalse(ApiVersion.parseRange("v1alpha1-v1").contains(ApiVersion.parse("v2alpha1")));
        assertFalse(ApiVersion.parseRange("v1alpha1-v1").contains(ApiVersion.parse("v2beta1")));
        assertFalse(ApiVersion.parseRange("v1alpha1-v1").contains(ApiVersion.parse("v2")));

        assertTrue(ApiVersion.parseRange("v1alpha1+").contains(ApiVersion.parse("v1alpha1")));
        assertTrue(ApiVersion.parseRange("v1alpha1+").contains(ApiVersion.parse("v1alpha2")));
        assertTrue(ApiVersion.parseRange("v1alpha1+").contains(ApiVersion.parse("v1beta1")));
        assertTrue(ApiVersion.parseRange("v1alpha1+").contains(ApiVersion.parse("v1beta2")));
        assertTrue(ApiVersion.parseRange("v1alpha1+").contains(ApiVersion.parse("v1")));
        assertFalse(ApiVersion.parseRange("v1alpha1+").contains(ApiVersion.parse("v0")));
        assertTrue(ApiVersion.parseRange("v1alpha1+").contains(ApiVersion.parse("v2alpha1")));
        assertTrue(ApiVersion.parseRange("v1alpha1+").contains(ApiVersion.parse("v2beta1")));
        assertTrue(ApiVersion.parseRange("v1alpha1+").contains(ApiVersion.parse("v2")));

        assertTrue(ApiVersion.parseRange("all").contains(ApiVersion.parse("v1alpha1")));
        assertTrue(ApiVersion.parseRange("all").contains(ApiVersion.parse("v1alpha2")));
        assertTrue(ApiVersion.parseRange("all").contains(ApiVersion.parse("v1beta1")));
        assertTrue(ApiVersion.parseRange("all").contains(ApiVersion.parse("v1beta2")));
        assertTrue(ApiVersion.parseRange("all").contains(ApiVersion.parse("v1")));
        assertTrue(ApiVersion.parseRange("all").contains(ApiVersion.parse("v0")));
        assertTrue(ApiVersion.parseRange("all").contains(ApiVersion.parse("v2alpha1")));
        assertTrue(ApiVersion.parseRange("all").contains(ApiVersion.parse("v2beta1")));
        assertTrue(ApiVersion.parseRange("all").contains(ApiVersion.parse("v2")));
    }

    @Test
    public void testIntersects() {
        assertTrue(ApiVersion.parseRange("v1").intersects(ApiVersion.parseRange("v1")));
        assertFalse(ApiVersion.parseRange("v1").intersects(ApiVersion.parseRange("v1alpha1")));
        assertFalse(ApiVersion.parseRange("v1").intersects(ApiVersion.parseRange("v2")));

        assertFalse(ApiVersion.parseRange("v1+").intersects(ApiVersion.parseRange("v0")));
        assertFalse(ApiVersion.parseRange("v1+").intersects(ApiVersion.parseRange("v1alpha1")));
        assertFalse(ApiVersion.parseRange("v1+").intersects(ApiVersion.parseRange("v1alpha2")));
        assertFalse(ApiVersion.parseRange("v1+").intersects(ApiVersion.parseRange("v1beta1")));
        assertFalse(ApiVersion.parseRange("v1+").intersects(ApiVersion.parseRange("v1beta2")));
        assertTrue(ApiVersion.parseRange("v1+").intersects(ApiVersion.parseRange("v1")));
        assertTrue(ApiVersion.parseRange("v1+").intersects(ApiVersion.parseRange("v2")));
        assertTrue(ApiVersion.parseRange("v1+").intersects(ApiVersion.parseRange("v4")));
        // swapped (symmetric operator)
        assertFalse(ApiVersion.parseRange("v0").intersects(ApiVersion.parseRange("v1+")));
        assertFalse(ApiVersion.parseRange("v1alpha1").intersects(ApiVersion.parseRange("v1+")));
        assertFalse(ApiVersion.parseRange("v1alpha2").intersects(ApiVersion.parseRange("v1+")));
        assertFalse(ApiVersion.parseRange("v1beta1").intersects(ApiVersion.parseRange("v1+")));
        assertFalse(ApiVersion.parseRange("v1beta2").intersects(ApiVersion.parseRange("v1+")));
        assertTrue(ApiVersion.parseRange("v1").intersects(ApiVersion.parseRange("v1+")));
        assertTrue(ApiVersion.parseRange("v2").intersects(ApiVersion.parseRange("v1+")));
        assertTrue(ApiVersion.parseRange("v4").intersects(ApiVersion.parseRange("v1+")));

        assertFalse(ApiVersion.parseRange("v1-v2").intersects(ApiVersion.parseRange("v0")));
        assertFalse(ApiVersion.parseRange("v1-v2").intersects(ApiVersion.parseRange("v1alpha1")));
        assertFalse(ApiVersion.parseRange("v1-v2").intersects(ApiVersion.parseRange("v1alpha2")));
        assertFalse(ApiVersion.parseRange("v1-v2").intersects(ApiVersion.parseRange("v1beta1")));
        assertFalse(ApiVersion.parseRange("v1-v2").intersects(ApiVersion.parseRange("v1beta2")));
        assertTrue(ApiVersion.parseRange("v1-v2").intersects(ApiVersion.parseRange("v1")));
        assertTrue(ApiVersion.parseRange("v1-v2").intersects(ApiVersion.parseRange("v2")));
        assertTrue(ApiVersion.parseRange("v1-v2").intersects(ApiVersion.parseRange("v2alpha1")));
        assertTrue(ApiVersion.parseRange("v1-v2").intersects(ApiVersion.parseRange("v2beta1")));
        assertFalse(ApiVersion.parseRange("v1-v2").intersects(ApiVersion.parseRange("v3alpha1")));
        assertFalse(ApiVersion.parseRange("v1-v2").intersects(ApiVersion.parseRange("v3")));
        // swapped (symmetric operator)
        assertFalse(ApiVersion.parseRange("v0").intersects(ApiVersion.parseRange("v1-v2")));
        assertFalse(ApiVersion.parseRange("v1alpha1").intersects(ApiVersion.parseRange("v1-v2")));
        assertFalse(ApiVersion.parseRange("v1alpha2").intersects(ApiVersion.parseRange("v1-v2")));
        assertFalse(ApiVersion.parseRange("v1beta1").intersects(ApiVersion.parseRange("v1-v2")));
        assertFalse(ApiVersion.parseRange("v1beta2").intersects(ApiVersion.parseRange("v1-v2")));
        assertTrue(ApiVersion.parseRange("v1").intersects(ApiVersion.parseRange("v1-v2")));
        assertTrue(ApiVersion.parseRange("v2").intersects(ApiVersion.parseRange("v1-v2")));
        assertTrue(ApiVersion.parseRange("v2alpha1").intersects(ApiVersion.parseRange("v1-v2")));
        assertTrue(ApiVersion.parseRange("v2beta1").intersects(ApiVersion.parseRange("v1-v2")));
        assertFalse(ApiVersion.parseRange("v3alpha1").intersects(ApiVersion.parseRange("v1-v2")));
        assertFalse(ApiVersion.parseRange("v3").intersects(ApiVersion.parseRange("v1-v2")));
    }
}
