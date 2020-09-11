/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ApiVersionRangeTest {
    @Test
    public void testContains() {

        assertTrue(ApiVersionRange.parse("v1").contains(ApiVersion.parse("v1")));
        assertFalse(ApiVersionRange.parse("v1").contains(ApiVersion.parse("v1alpha1")));
        assertFalse(ApiVersionRange.parse("v1").contains(ApiVersion.parse("v1beta1")));
        assertFalse(ApiVersionRange.parse("v1").contains(ApiVersion.parse("v2")));

        assertTrue(ApiVersionRange.parse("v1alpha1-v1").contains(ApiVersion.parse("v1alpha1")));
        assertTrue(ApiVersionRange.parse("v1alpha1-v1").contains(ApiVersion.parse("v1alpha2")));
        assertTrue(ApiVersionRange.parse("v1alpha1-v1").contains(ApiVersion.parse("v1beta1")));
        assertTrue(ApiVersionRange.parse("v1alpha1-v1").contains(ApiVersion.parse("v1beta2")));
        assertTrue(ApiVersionRange.parse("v1alpha1-v1").contains(ApiVersion.parse("v1")));
        assertFalse(ApiVersionRange.parse("v1alpha1-v1").contains(ApiVersion.parse("v0")));
        assertFalse(ApiVersionRange.parse("v1alpha1-v1").contains(ApiVersion.parse("v2alpha1")));
        assertFalse(ApiVersionRange.parse("v1alpha1-v1").contains(ApiVersion.parse("v2beta1")));
        assertFalse(ApiVersionRange.parse("v1alpha1-v1").contains(ApiVersion.parse("v2")));

        assertTrue(ApiVersionRange.parse("v1alpha1+").contains(ApiVersion.parse("v1alpha1")));
        assertTrue(ApiVersionRange.parse("v1alpha1+").contains(ApiVersion.parse("v1alpha2")));
        assertTrue(ApiVersionRange.parse("v1alpha1+").contains(ApiVersion.parse("v1beta1")));
        assertTrue(ApiVersionRange.parse("v1alpha1+").contains(ApiVersion.parse("v1beta2")));
        assertTrue(ApiVersionRange.parse("v1alpha1+").contains(ApiVersion.parse("v1")));
        assertFalse(ApiVersionRange.parse("v1alpha1+").contains(ApiVersion.parse("v0")));
        assertTrue(ApiVersionRange.parse("v1alpha1+").contains(ApiVersion.parse("v2alpha1")));
        assertTrue(ApiVersionRange.parse("v1alpha1+").contains(ApiVersion.parse("v2beta1")));
        assertTrue(ApiVersionRange.parse("v1alpha1+").contains(ApiVersion.parse("v2")));

        assertTrue(ApiVersionRange.parse("all").contains(ApiVersion.parse("v1alpha1")));
        assertTrue(ApiVersionRange.parse("all").contains(ApiVersion.parse("v1alpha2")));
        assertTrue(ApiVersionRange.parse("all").contains(ApiVersion.parse("v1beta1")));
        assertTrue(ApiVersionRange.parse("all").contains(ApiVersion.parse("v1beta2")));
        assertTrue(ApiVersionRange.parse("all").contains(ApiVersion.parse("v1")));
        assertTrue(ApiVersionRange.parse("all").contains(ApiVersion.parse("v0")));
        assertTrue(ApiVersionRange.parse("all").contains(ApiVersion.parse("v2alpha1")));
        assertTrue(ApiVersionRange.parse("all").contains(ApiVersion.parse("v2beta1")));
        assertTrue(ApiVersionRange.parse("all").contains(ApiVersion.parse("v2")));
    }

    @Test
    public void testIntersects() {
        assertTrue(ApiVersionRange.parse("v1").intersects(ApiVersionRange.parse("v1")));
        assertFalse(ApiVersionRange.parse("v1").intersects(ApiVersionRange.parse("v1alpha1")));
        assertFalse(ApiVersionRange.parse("v1").intersects(ApiVersionRange.parse("v2")));

        assertFalse(ApiVersionRange.parse("v1+").intersects(ApiVersionRange.parse("v0")));
        assertFalse(ApiVersionRange.parse("v1+").intersects(ApiVersionRange.parse("v1alpha1")));
        assertFalse(ApiVersionRange.parse("v1+").intersects(ApiVersionRange.parse("v1alpha2")));
        assertFalse(ApiVersionRange.parse("v1+").intersects(ApiVersionRange.parse("v1beta1")));
        assertFalse(ApiVersionRange.parse("v1+").intersects(ApiVersionRange.parse("v1beta2")));
        assertTrue(ApiVersionRange.parse("v1+").intersects(ApiVersionRange.parse("v1")));
        assertTrue(ApiVersionRange.parse("v1+").intersects(ApiVersionRange.parse("v2")));
        assertTrue(ApiVersionRange.parse("v1+").intersects(ApiVersionRange.parse("v4")));
        // swapped (symmetric operator)
        assertFalse(ApiVersionRange.parse("v0").intersects(ApiVersionRange.parse("v1+")));
        assertFalse(ApiVersionRange.parse("v1alpha1").intersects(ApiVersionRange.parse("v1+")));
        assertFalse(ApiVersionRange.parse("v1alpha2").intersects(ApiVersionRange.parse("v1+")));
        assertFalse(ApiVersionRange.parse("v1beta1").intersects(ApiVersionRange.parse("v1+")));
        assertFalse(ApiVersionRange.parse("v1beta2").intersects(ApiVersionRange.parse("v1+")));
        assertTrue(ApiVersionRange.parse("v1").intersects(ApiVersionRange.parse("v1+")));
        assertTrue(ApiVersionRange.parse("v2").intersects(ApiVersionRange.parse("v1+")));
        assertTrue(ApiVersionRange.parse("v4").intersects(ApiVersionRange.parse("v1+")));

        assertFalse(ApiVersionRange.parse("v1-v2").intersects(ApiVersionRange.parse("v0")));
        assertFalse(ApiVersionRange.parse("v1-v2").intersects(ApiVersionRange.parse("v1alpha1")));
        assertFalse(ApiVersionRange.parse("v1-v2").intersects(ApiVersionRange.parse("v1alpha2")));
        assertFalse(ApiVersionRange.parse("v1-v2").intersects(ApiVersionRange.parse("v1beta1")));
        assertFalse(ApiVersionRange.parse("v1-v2").intersects(ApiVersionRange.parse("v1beta2")));
        assertTrue(ApiVersionRange.parse("v1-v2").intersects(ApiVersionRange.parse("v1")));
        assertTrue(ApiVersionRange.parse("v1-v2").intersects(ApiVersionRange.parse("v2")));
        assertTrue(ApiVersionRange.parse("v1-v2").intersects(ApiVersionRange.parse("v2alpha1")));
        assertTrue(ApiVersionRange.parse("v1-v2").intersects(ApiVersionRange.parse("v2beta1")));
        assertFalse(ApiVersionRange.parse("v1-v2").intersects(ApiVersionRange.parse("v3alpha1")));
        assertFalse(ApiVersionRange.parse("v1-v2").intersects(ApiVersionRange.parse("v3")));
        // swapped (symmetric operator)
        assertFalse(ApiVersionRange.parse("v0").intersects(ApiVersionRange.parse("v1-v2")));
        assertFalse(ApiVersionRange.parse("v1alpha1").intersects(ApiVersionRange.parse("v1-v2")));
        assertFalse(ApiVersionRange.parse("v1alpha2").intersects(ApiVersionRange.parse("v1-v2")));
        assertFalse(ApiVersionRange.parse("v1beta1").intersects(ApiVersionRange.parse("v1-v2")));
        assertFalse(ApiVersionRange.parse("v1beta2").intersects(ApiVersionRange.parse("v1-v2")));
        assertTrue(ApiVersionRange.parse("v1").intersects(ApiVersionRange.parse("v1-v2")));
        assertTrue(ApiVersionRange.parse("v2").intersects(ApiVersionRange.parse("v1-v2")));
        assertTrue(ApiVersionRange.parse("v2alpha1").intersects(ApiVersionRange.parse("v1-v2")));
        assertTrue(ApiVersionRange.parse("v2beta1").intersects(ApiVersionRange.parse("v1-v2")));
        assertFalse(ApiVersionRange.parse("v3alpha1").intersects(ApiVersionRange.parse("v1-v2")));
        assertFalse(ApiVersionRange.parse("v3").intersects(ApiVersionRange.parse("v1-v2")));
    }
}
