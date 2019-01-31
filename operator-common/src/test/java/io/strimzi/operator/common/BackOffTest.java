/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BackOffTest {

    @Test
    public void testDefaultBackoff() {
        BackOff b = new BackOff();
        assertEquals(6200L, b.totalDelayMs());
        assertFalse(b.done());
        assertEquals(0L, b.delayMs());
        assertEquals(0L, b.cumulativeDelayMs());
        assertFalse(b.done());
        assertEquals(200L, b.delayMs());
        assertEquals(200L, b.cumulativeDelayMs());
        assertFalse(b.done());
        assertEquals(400L, b.delayMs());
        assertEquals(600L, b.cumulativeDelayMs());
        assertFalse(b.done());
        assertEquals(800L, b.delayMs());
        assertEquals(1400L, b.cumulativeDelayMs());
        assertFalse(b.done());
        assertEquals(1600L, b.delayMs());
        assertEquals(3000L, b.cumulativeDelayMs());
        assertFalse(b.done());
        assertEquals(3200L, b.delayMs());
        assertEquals(6200L, b.cumulativeDelayMs());
        assertTrue(b.done());
        try {
            b.delayMs();
            fail("Should throw");
        } catch (MaxAttemptsExceededException e) {

        }
        assertTrue(b.done());
        assertEquals(6200L, b.totalDelayMs());
    }

    @Test
    public void testAnotherBackoff() {
        BackOff b = new BackOff(1, 10, 5);
        assertEquals(1111L, b.totalDelayMs());
        //attempt
        assertEquals(0L, b.delayMs());
        //attempt
        assertEquals(1L, b.delayMs());
        //attempt
        assertEquals(10L, b.delayMs());
        //attempt
        assertEquals(100L, b.delayMs());
        //attempt
        assertEquals(1000L, b.delayMs());
        try {
            b.delayMs();
            fail("Should throw");
        } catch (MaxAttemptsExceededException e) {

        }

        assertEquals(1111L, b.totalDelayMs());
    }

    @Test
    public void testMaxAttemptsOnlyBackOff() {
        BackOff b = new BackOff(3);
        assertEquals(0, b.delayMs());
        assertEquals(200L, b.delayMs());
        assertEquals(400L, b.delayMs());
        try {
            b.delayMs();
            fail("Should throw");
        } catch (MaxAttemptsExceededException e) {

        }
        assertEquals(600L, b.totalDelayMs());
    }
}