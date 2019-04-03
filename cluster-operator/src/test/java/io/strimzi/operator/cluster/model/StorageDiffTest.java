/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.JbodStorageBuilder;
import io.strimzi.api.kafka.model.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.Storage;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StorageDiffTest {
    @Test
    public void testJbodDiff()    {
        Storage jbod = new JbodStorageBuilder().withVolumes(
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build(),
                new PersistentClaimStorageBuilder().withStorageClass("gp2-st1").withDeleteClaim(true).withId(1).withSize("1000Gi").build())
                .build();

        Storage jbod2 = new JbodStorageBuilder().withVolumes(
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("1000Gi").build(),
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(true).withId(1).withSize("1000Gi").build())
                .build();

        assertFalse(new StorageDiff(jbod, jbod).changesType());
        assertTrue(new StorageDiff(jbod, jbod).isEmpty());
        assertFalse(new StorageDiff(jbod, jbod).changesSize());

        assertFalse(new StorageDiff(jbod, jbod2).changesType());
        assertFalse(new StorageDiff(jbod, jbod2).isEmpty());
        assertTrue(new StorageDiff(jbod, jbod2).changesSize());
    }

    @Test
    public void testPersistentDiff()    {
        Storage persistent = new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build();
        Storage persistent2 = new PersistentClaimStorageBuilder().withStorageClass("gp2-st1").withDeleteClaim(false).withId(0).withSize("1000Gi").build();

        assertFalse(new StorageDiff(persistent, persistent).changesType());
        assertTrue(new StorageDiff(persistent, persistent).isEmpty());
        assertFalse(new StorageDiff(persistent, persistent).changesSize());

        assertFalse(new StorageDiff(persistent, persistent2).changesType());
        assertFalse(new StorageDiff(persistent, persistent2).isEmpty());
        assertTrue(new StorageDiff(persistent, persistent2).changesSize());
    }

    @Test
    public void testEphemeralDiff()    {
        Storage ephemeral = new EphemeralStorageBuilder().build();

        assertFalse(new StorageDiff(ephemeral, ephemeral).changesType());
        assertTrue(new StorageDiff(ephemeral, ephemeral).isEmpty());
        assertFalse(new StorageDiff(ephemeral, ephemeral).changesSize());
    }

    @Test
    public void testCrossDiff()    {
        Storage jbod = new JbodStorageBuilder().withVolumes(
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build(),
                new PersistentClaimStorageBuilder().withStorageClass("gp2-st1").withDeleteClaim(true).withId(1).withSize("1000Gi").build())
                .build();

        Storage ephemeral = new EphemeralStorageBuilder().build();

        Storage persistent = new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build();

        assertTrue(new StorageDiff(jbod, ephemeral).changesType());
        assertTrue(new StorageDiff(persistent, ephemeral).changesType());
        assertTrue(new StorageDiff(jbod, persistent).changesType());

        assertFalse(new StorageDiff(jbod, ephemeral).isEmpty());
        assertFalse(new StorageDiff(persistent, ephemeral).isEmpty());
        assertFalse(new StorageDiff(jbod, persistent).isEmpty());
    }
}
