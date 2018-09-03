/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PasswordGeneratorTest {

    @Test
    public void length() {
        PasswordGenerator generator = new PasswordGenerator(10, "a");
        assertEquals("aaaaaaaaaa", generator.generate());
    }

    @Test
    public void alphabet() {
        PasswordGenerator generator = new PasswordGenerator(10, "ab");
        assertTrue(generator.generate().matches("[ab]{10}"));
    }
}
