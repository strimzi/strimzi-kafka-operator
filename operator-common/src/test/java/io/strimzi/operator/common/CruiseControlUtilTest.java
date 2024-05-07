/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CruiseControlUtilTest {
    @Test
    public void testBuildBasicAuthValue() {
        String username = "testUser";
        String password = "testPassword";
        String expectedHeaderValue = "Basic dGVzdFVzZXI6dGVzdFBhc3N3b3Jk";
        String actualHeaderValue = CruiseControlUtil.buildBasicAuthValue(username, password);
        assertEquals(expectedHeaderValue, actualHeaderValue);
    }
}
