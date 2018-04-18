/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ResourcesTest {

    @Test
    public void testDeserializeSuffixes() {
        Resources opts = Resources.fromJson("{\"limits\": {\"memory\": \"10Gi\"}, \"requests\": {\"memory\": \"5G\"}}");
        assertEquals(10737418240L, opts.getLimits().getMemory());
        assertEquals(5000000000L, opts.getRequests().getMemory());
    }

    @Test
    public void testDeserializeInts() {
        Resources opts = Resources.fromJson("{\"limits\": {\"memory\": 10737418240}, \"requests\": {\"memory\": 5000000000}}");
        assertEquals(10737418240L, opts.getLimits().getMemory());
        assertEquals(5000000000L, opts.getRequests().getMemory());
    }

    @Test
    public void testDeserializeDefaults() {
        Resources opts = Resources.fromJson("{\"limits\": {\"memory\": 10737418240}, \"requests\": {} }");
        assertEquals(10737418240L, opts.getLimits().getMemory());
        assertEquals(0, opts.getRequests().getMemory());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeserializeInvalidMemory() {
        Resources.fromJson("{\"limits\": {\"memory\": \"foo\"}, \"requests\": {\"memory\": bar}}");
    }
}
