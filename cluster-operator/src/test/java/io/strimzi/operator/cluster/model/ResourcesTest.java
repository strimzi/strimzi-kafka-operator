/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ResourcesTest {

    @Test
    public void testDeserializeSuffixes() {
        Resources opts = Resources.fromJson("{\"limits\": {\"memory\": \"10Gi\", \"cpu\": \"1\"}, \"requests\": {\"memory\": \"5G\", \"cpu\": 1}}");
        assertEquals(10737418240L, opts.getLimits().getMemory());
        assertEquals(1000, opts.getLimits().getMilliCpu());
        assertEquals("1", opts.getLimits().getCpuFormatted());
        assertEquals(5000000000L, opts.getRequests().getMemory());
        assertEquals(1000, opts.getLimits().getMilliCpu());
        assertEquals("1", opts.getLimits().getCpuFormatted());
        AbstractModel abstractModel = new AbstractModel("", "", Labels.forCluster("")) {
        };
        abstractModel.setResources(opts);
        assertEquals("1", abstractModel.resources().getLimits().get("cpu").getAmount());
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
