/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AbstractModelTest {

    private static JvmOptions jvmOptions(String xmx, String xms) {
        JvmOptions result = new JvmOptions();
        result.setXms(xms);
        result.setXmx(xmx);
        return result;
    }

    @Test
    public void testJvmMemoryOptionsExplicit() {
        AbstractModel am = new AbstractModel(null, null, Labels.forCluster("foo")) { };
        am.setJvmOptions(jvmOptions("4", "4"));
        assertEquals("-Xms4 -Xmx4", am.javaHeapOptions(5_000_000_000L, 0.5));
    }

    @Test
    public void testJvmMemoryOptionsDefault() {
        AbstractModel am = new AbstractModel(null, null, Labels.forCluster("foo")) { };
        am.setJvmOptions(jvmOptions(null, "4"));
        assertEquals("-Xms4 " +
                        "-XX:+UnlockExperimentalVMOptions " +
                        "-XX:+UseCGroupMemoryLimitForHeap " +
                        "-XX:MaxRAMFraction=2",
                am.javaHeapOptions(5_000_000_000L, 0.5));
        assertEquals("-Xms4 " +
                        "-XX:+UnlockExperimentalVMOptions " +
                        "-XX:+UseCGroupMemoryLimitForHeap " +
                        "-XX:MaxRAMFraction=2",
                am.javaHeapOptions(5_000_000_000L, 0.7));
        assertEquals("-Xms4 " +
                        "-XX:+UnlockExperimentalVMOptions " +
                        "-XX:+UseCGroupMemoryLimitForHeap " +
                        "-XX:MaxRAMFraction=1",
                am.javaHeapOptions(5_000_000_000L, 1.0));
    }

    @Test
    public void testJvmMemoryOptionsMemoryRequest() {
        AbstractModel am = new AbstractModel(null, null, Labels.forCluster("foo")) { };
        am.setJvmOptions(jvmOptions(null, null));

        am.setResources(new Resources(null, new Resources.CpuMemory(4_000_000_000L, 0)));
        assertEquals("-XX:MaxRAM=2000000000",
                am.javaHeapOptions(5_000_000_000L, 0.5));
        am.setResources(new Resources(null, new Resources.CpuMemory(5_000_000_000L, 0)));
        assertEquals("-XX:MaxRAM=2500000000",
                am.javaHeapOptions(5_000_000_000L, 0.5));
        am.setResources(new Resources(null, new Resources.CpuMemory(10_000_000_000L, 0)));
        assertEquals("-XX:MaxRAM=5000000000",
                am.javaHeapOptions(5_000_000_000L, 0.5));
        am.setResources(new Resources(null, new Resources.CpuMemory(12_000_000_000L, 0)));
        assertEquals("-XX:MaxRAM=5000000000",
                am.javaHeapOptions(5_000_000_000L, 0.5));
        am.setResources(new Resources(null, new Resources.CpuMemory(100_000_000_000L, 0)));
        assertEquals("-XX:MaxRAM=5000000000",
                am.javaHeapOptions(5_000_000_000L, 0.5));
    }
}
