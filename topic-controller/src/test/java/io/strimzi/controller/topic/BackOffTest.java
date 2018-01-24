/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.controller.topic;


import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BackOffTest {

    @Test
    public void testDefaultBackoff() {
        BackOff b = new BackOff();
        assertEquals(0L, b.delayMs());
        assertEquals(200L, b.delayMs());
        assertEquals(400L, b.delayMs());
        assertEquals(800L, b.delayMs());
        try {
            b.delayMs();
            fail("Should throw");
        } catch (MaxAttemptsExceededException e) {

        }
        assertEquals(1400L, b.totalDelayMs());
    }

    @Test
    public void testAnotherBackoff() {
        BackOff b = new BackOff(1, 10, 5);
        assertEquals(0L, b.delayMs());
        assertEquals(1L, b.delayMs());
        assertEquals(10L, b.delayMs());
        assertEquals(100L, b.delayMs());
        assertEquals(1000L, b.delayMs());
        try {
            b.delayMs();
            fail("Should throw");
        } catch (MaxAttemptsExceededException e) {

        }

        assertEquals(1111L, b.totalDelayMs());
    }
}