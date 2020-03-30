/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BackOffTest {

    @Test
    public void testDefaultBackoff() {
        BackOff b = new BackOff();
        assertThat(b.totalDelayMs(), is(6200L));
        assertThat(b.done(), is(false));
        assertThat(b.delayMs(), is(0L));
        assertThat(b.cumulativeDelayMs(), is(0L));
        assertThat(b.done(), is(false));
        assertThat(b.delayMs(), is(200L));
        assertThat(b.cumulativeDelayMs(), is(200L));
        assertThat(b.done(), is(false));
        assertThat(b.delayMs(), is(400L));
        assertThat(b.cumulativeDelayMs(), is(600L));
        assertThat(b.done(), is(false));
        assertThat(b.delayMs(), is(800L));
        assertThat(b.cumulativeDelayMs(), is(1400L));
        assertThat(b.done(), is(false));
        assertThat(b.delayMs(), is(1600L));
        assertThat(b.cumulativeDelayMs(), is(3000L));
        assertThat(b.done(), is(false));
        assertThat(b.delayMs(), is(3200L));
        assertThat(b.cumulativeDelayMs(), is(6200L));
        assertThat(b.done(), is(true));

        assertThrows(MaxAttemptsExceededException.class, () -> b.delayMs());

        assertThat(b.done(), is(true));
        assertThat(b.totalDelayMs(), is(6200L));
    }

    @Test
    public void testAnotherBackoff() {
        BackOff b = new BackOff(1, 10, 5);
        assertThat(b.totalDelayMs(), is(1111L));
        //attempt
        assertThat(b.delayMs(), is(0L));
        //attempt
        assertThat(b.delayMs(), is(1L));
        //attempt
        assertThat(b.delayMs(), is(10L));
        //attempt
        assertThat(b.delayMs(), is(100L));
        //attempt
        assertThat(b.delayMs(), is(1000L));

        assertThrows(MaxAttemptsExceededException.class, () -> b.delayMs());

        assertThat(b.totalDelayMs(), is(1111L));
    }

    @Test
    public void testMaxAttemptsOnlyBackOff() {
        BackOff b = new BackOff(3);
        assertThat(b.delayMs(), is(0L));
        assertThat(b.delayMs(), is(200L));
        assertThat(b.delayMs(), is(400L));

        assertThrows(MaxAttemptsExceededException.class, () -> b.delayMs());

        assertThat(b.totalDelayMs(), is(600L));
    }
}