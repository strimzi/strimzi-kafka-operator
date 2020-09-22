/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.annotations;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ApiVersionTest {

    @Test
    public void testOrdering() {
        ApiVersion v1 = ApiVersion.parse("v1");
        ApiVersion v2 = ApiVersion.parse("v2");
        ApiVersion v1alpha1 = ApiVersion.parse("v1alpha1");
        ApiVersion v1alpha2 = ApiVersion.parse("v1alpha2");
        ApiVersion v1beta1 = ApiVersion.parse("v1beta1");
        ApiVersion v1beta2 = ApiVersion.parse("v1beta2");
        for (ApiVersion x : new ApiVersion[]{v1alpha1, v1alpha2, v1beta1, v1beta2, v1, v2}) {
            for (ApiVersion y : new ApiVersion[]{v1alpha1, v1alpha2, v1beta1, v1beta2, v1, v2}) {
                if (x.equals(y)) {
                    assertTrue(x.compareTo(y) == 0);
                    break;
                }
                assertTrue(x.compareTo(y) > 0, x + " > " + y);
            }
        }
    }
}
