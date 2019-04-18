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

        StorageDiff diff = new StorageDiff(jbod, jbod);
        assertFalse(diff.changesType());
        assertTrue(diff.isEmpty());
        assertFalse(diff.changesSize());

        diff = new StorageDiff(jbod, jbod2);
        assertFalse(diff.changesType());
        assertFalse(diff.isEmpty());
        assertTrue(diff.changesSize());
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

        StorageDiff diffJbodEphemeral = new StorageDiff(jbod, ephemeral);
        StorageDiff diffPersistentEphemeral = new StorageDiff(persistent, ephemeral);
        StorageDiff fiddJbodPersistent = new StorageDiff(jbod, persistent);

        assertTrue(diffJbodEphemeral.changesType());
        assertTrue(diffPersistentEphemeral.changesType());
        assertTrue(fiddJbodPersistent.changesType());

        assertFalse(diffJbodEphemeral.isEmpty());
        assertFalse(diffPersistentEphemeral.isEmpty());
        assertFalse(fiddJbodPersistent.isEmpty());
    }

    @Test
    public void testJbodDiffWithNewVolume()    {
        Storage jbod = new JbodStorageBuilder()
                .withVolumes(new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("1000Gi").build())
                .build();

        Storage jbod2 = new JbodStorageBuilder().withVolumes(
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("1000Gi").build(),
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(true).withId(1).withSize("1000Gi").build())
                .build();

        Storage jbod3 = new JbodStorageBuilder().withVolumes(
                new PersistentClaimStorageBuilder().withStorageClass("gp2-st1").withDeleteClaim(false).withId(0).withSize("100Gi").build(),
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(true).withId(1).withSize("1000Gi").build())
                .build();

        Storage jbod4 = new JbodStorageBuilder()
                .withVolumes(new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(2).withSize("1000Gi").build())
                .build();

        Storage jbod5 = new JbodStorageBuilder().withVolumes(
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(true).withId(1).withSize("1000Gi").build())
                .build();

        Storage jbod6 = new JbodStorageBuilder().withVolumes(
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("1000Gi").build(),
                new PersistentClaimStorageBuilder().withStorageClass("gp2-st1").withDeleteClaim(true).withId(2).withSize("5000Gi").build())
                .build();

        // Volume added
        StorageDiff diff = new StorageDiff(jbod, jbod2);
        assertFalse(diff.changesType());
        assertTrue(diff.isEmpty());
        assertFalse(diff.changesSize());

        // Volume removed
        diff = new StorageDiff(jbod2, jbod);
        assertFalse(diff.changesType());
        assertTrue(diff.isEmpty());
        assertFalse(diff.changesSize());

        // Volume added with changes
        diff = new StorageDiff(jbod, jbod3);
        assertFalse(diff.changesType());
        assertFalse(diff.isEmpty());
        assertTrue(diff.changesSize());

        // Volume removed from the beginning
        diff = new StorageDiff(jbod3, jbod5);
        assertFalse(diff.changesType());
        assertTrue(diff.isEmpty());
        assertFalse(diff.changesSize());

        // Volume added to the beginning
        diff = new StorageDiff(jbod5, jbod3);
        assertFalse(diff.changesType());
        assertTrue(diff.isEmpty());
        assertFalse(diff.changesSize());

        // Volume replaced with another ID and another volume which is kept changed
        diff = new StorageDiff(jbod3, jbod6);
        assertFalse(diff.changesType());
        assertFalse(diff.isEmpty());
        assertTrue(diff.changesSize());

        // Volume replaced with another ID in single volume broker
        diff = new StorageDiff(jbod, jbod4);
        assertFalse(diff.changesType());
        assertTrue(diff.isEmpty());
        assertFalse(diff.changesSize());

        // Volume replaced with another ID without chenging the volumes which are kept
        diff = new StorageDiff(jbod2, jbod6);
        assertFalse(diff.changesType());
        assertTrue(diff.isEmpty());
        assertFalse(diff.changesSize());
    }
}
