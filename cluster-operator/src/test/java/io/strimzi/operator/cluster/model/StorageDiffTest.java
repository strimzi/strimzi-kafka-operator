/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.storage.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.storage.JbodStorageBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageOverrideBuilder;
import io.strimzi.api.kafka.model.storage.Storage;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

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
        assertThat(diff.changesType(), is(false));
        assertThat(diff.isEmpty(), is(true));
        assertThat(diff.shrinkSize(), is(false));

        diff = new StorageDiff(jbod, jbod2);
        assertThat(diff.changesType(), is(false));
        assertThat(diff.isEmpty(), is(false));
        assertThat(diff.shrinkSize(), is(false));
    }

    @Test
    public void testPersistentDiff()    {
        Storage persistent = new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build();
        Storage persistent2 = new PersistentClaimStorageBuilder().withStorageClass("gp2-st1").withDeleteClaim(false).withId(0).withSize("1000Gi").build();

        assertThat(new StorageDiff(persistent, persistent).changesType(), is(false));
        assertThat(new StorageDiff(persistent, persistent).isEmpty(), is(true));
        assertThat(new StorageDiff(persistent, persistent).shrinkSize(), is(false));

        assertThat(new StorageDiff(persistent, persistent2).changesType(), is(false));
        assertThat(new StorageDiff(persistent, persistent2).isEmpty(), is(false));
        assertThat(new StorageDiff(persistent, persistent2).shrinkSize(), is(false));
    }

    @Test
    public void testPersistentDiffWithOverrides()    {
        Storage persistent = new PersistentClaimStorageBuilder()
                .withStorageClass("gp2-ssd")
                .withDeleteClaim(false)
                .withId(0)
                .withSize("100Gi")
                .build();
        Storage persistent2 = new PersistentClaimStorageBuilder()
                .withStorageClass("gp2-ssd")
                .withDeleteClaim(false)
                .withId(0)
                .withSize("100Gi")
                .withOverrides(new PersistentClaimStorageOverrideBuilder()
                        .withBroker(1)
                        .withStorageClass("gp2-ssd-az1")
                        .build())
                .build();
        Storage persistent3 = new PersistentClaimStorageBuilder()
                .withStorageClass("gp2-ssd")
                .withDeleteClaim(false)
                .withId(0)
                .withSize("100Gi")
                .withOverrides(new PersistentClaimStorageOverrideBuilder()
                        .withBroker(1)
                        .withStorageClass("gp2-ssd-az2")
                        .build())
                .build();

        assertThat(new StorageDiff(persistent, persistent).changesType(), is(false));
        assertThat(new StorageDiff(persistent, persistent).isEmpty(), is(true));
        assertThat(new StorageDiff(persistent, persistent).shrinkSize(), is(false));

        assertThat(new StorageDiff(persistent, persistent2).changesType(), is(false));
        assertThat(new StorageDiff(persistent, persistent2).isEmpty(), is(false));
        assertThat(new StorageDiff(persistent, persistent2).shrinkSize(), is(false));

        assertThat(new StorageDiff(persistent2, persistent3).changesType(), is(false));
        assertThat(new StorageDiff(persistent2, persistent3).isEmpty(), is(false));
        assertThat(new StorageDiff(persistent2, persistent3).shrinkSize(), is(false));
    }

    @Test
    public void testSizeChanges()    {
        Storage persistent = new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build();
        Storage persistent2 = new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("1000Gi").build();
        Storage persistent3 = new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("10Gi").build();

        assertThat(new StorageDiff(persistent, persistent).shrinkSize(), is(false));
        assertThat(new StorageDiff(persistent, persistent2).shrinkSize(), is(false));
        assertThat(new StorageDiff(persistent, persistent3).shrinkSize(), is(true));
    }

    @Test
    public void testEphemeralDiff()    {
        Storage ephemeral = new EphemeralStorageBuilder().build();

        assertThat(new StorageDiff(ephemeral, ephemeral).changesType(), is(false));
        assertThat(new StorageDiff(ephemeral, ephemeral).isEmpty(), is(true));
        assertThat(new StorageDiff(ephemeral, ephemeral).shrinkSize(), is(false));
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

        assertThat(diffJbodEphemeral.changesType(), is(true));
        assertThat(diffPersistentEphemeral.changesType(), is(true));
        assertThat(fiddJbodPersistent.changesType(), is(true));

        assertThat(diffJbodEphemeral.isEmpty(), is(false));
        assertThat(diffPersistentEphemeral.isEmpty(), is(false));
        assertThat(fiddJbodPersistent.isEmpty(), is(false));
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
        assertThat(diff.changesType(), is(false));
        assertThat(diff.isEmpty(), is(true));
        assertThat(diff.shrinkSize(), is(false));

        // Volume removed
        diff = new StorageDiff(jbod2, jbod);
        assertThat(diff.changesType(), is(false));
        assertThat(diff.isEmpty(), is(true));
        assertThat(diff.shrinkSize(), is(false));

        // Volume added with changes
        diff = new StorageDiff(jbod, jbod3);
        assertThat(diff.changesType(), is(false));
        assertThat(diff.isEmpty(), is(false));
        assertThat(diff.shrinkSize(), is(true));

        // Volume removed from the beginning
        diff = new StorageDiff(jbod3, jbod5);
        assertThat(diff.changesType(), is(false));
        assertThat(diff.isEmpty(), is(true));
        assertThat(diff.shrinkSize(), is(false));

        // Volume added to the beginning
        diff = new StorageDiff(jbod5, jbod3);
        assertThat(diff.changesType(), is(false));
        assertThat(diff.isEmpty(), is(true));
        assertThat(diff.shrinkSize(), is(false));

        // Volume replaced with another ID and another volume which is kept changed
        diff = new StorageDiff(jbod3, jbod6);
        assertThat(diff.changesType(), is(false));
        assertThat(diff.isEmpty(), is(false));
        assertThat(diff.shrinkSize(), is(false));

        // Volume replaced with another ID in single volume broker
        diff = new StorageDiff(jbod, jbod4);
        assertThat(diff.changesType(), is(false));
        assertThat(diff.isEmpty(), is(true));
        assertThat(diff.shrinkSize(), is(false));

        // Volume replaced with another ID without chenging the volumes which are kept
        diff = new StorageDiff(jbod2, jbod6);
        assertThat(diff.changesType(), is(false));
        assertThat(diff.isEmpty(), is(true));
        assertThat(diff.shrinkSize(), is(false));
    }

    @Test
    public void testSizeChangesInJbod()    {
        Storage jbod = new JbodStorageBuilder().withVolumes(
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("1000Gi").build(),
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(true).withId(1).withSize("1000Gi").build())
                .build();

        Storage jbod2 = new JbodStorageBuilder().withVolumes(
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("5000Gi").build(),
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(true).withId(1).withSize("1000Gi").build())
                .build();

        Storage jbod3 = new JbodStorageBuilder().withVolumes(
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("1000Gi").build(),
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(true).withId(1).withSize("500Gi").build())
                .build();

        assertThat(new StorageDiff(jbod, jbod).shrinkSize(), is(false));
        assertThat(new StorageDiff(jbod, jbod2).shrinkSize(), is(false));
        assertThat(new StorageDiff(jbod, jbod3).shrinkSize(), is(true));
    }
}
