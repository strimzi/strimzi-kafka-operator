/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.kafka.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageOverrideBuilder;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
public class StorageDiffTest {
    @ParallelTest
    public void testJbodDiff()    {
        Storage jbod = new JbodStorageBuilder().withVolumes(
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build(),
                new PersistentClaimStorageBuilder().withStorageClass("gp2-st1").withDeleteClaim(true).withId(1).withSize("1000Gi").build())
                .build();

        Storage jbod2 = new JbodStorageBuilder().withVolumes(
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("1000Gi").build(),
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(true).withId(1).withSize("1000Gi").build())
                .build();

        StorageDiff diff = new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, jbod, jbod, Set.of(0, 1, 5), Set.of(0, 1, 5));
        assertThat(diff.changesType(), is(false));
        assertThat(diff.isEmpty(), is(true));
        assertThat(diff.shrinkSize(), is(false));
        assertThat(diff.isVolumesAddedOrRemoved(), is(false));

        diff = new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, jbod, jbod2, Set.of(0, 1, 5), Set.of(0, 1, 5));
        assertThat(diff.changesType(), is(false));
        assertThat(diff.isEmpty(), is(false));
        assertThat(diff.shrinkSize(), is(false));
        assertThat(diff.isVolumesAddedOrRemoved(), is(false));
    }

    @ParallelTest
    public void testPersistentDiff()    {
        Storage persistent = new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build();
        Storage persistent2 = new PersistentClaimStorageBuilder().withStorageClass("gp2-st1").withDeleteClaim(false).withId(0).withSize("1000Gi").build();

        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent, Set.of(0, 1, 5), Set.of(0, 1, 5)).changesType(), is(false));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent, Set.of(0, 1, 5), Set.of(0, 1, 5)).isEmpty(), is(true));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent, Set.of(0, 1, 5), Set.of(0, 1, 5)).shrinkSize(), is(false));

        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent2, Set.of(0, 1, 5), Set.of(0, 1, 5)).changesType(), is(false));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent2, Set.of(0, 1, 5), Set.of(0, 1, 5)).isEmpty(), is(false));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent2, Set.of(0, 1, 5), Set.of(0, 1, 5)).shrinkSize(), is(false));
    }

    @ParallelTest
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

        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent, Set.of(0, 1, 5), Set.of(0, 1, 5)).changesType(), is(false));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent, Set.of(0, 1, 5), Set.of(0, 1, 5)).isEmpty(), is(true));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent, Set.of(0, 1, 5), Set.of(0, 1, 5)).shrinkSize(), is(false));

        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent2, Set.of(0, 1, 5), Set.of(0, 1, 5)).changesType(), is(false));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent2, Set.of(0, 1, 5), Set.of(0, 1, 5)).isEmpty(), is(false));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent2, Set.of(0, 1, 5), Set.of(0, 1, 5)).shrinkSize(), is(false));

        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent2, persistent3, Set.of(0, 1, 5), Set.of(0, 1, 5)).changesType(), is(false));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent2, persistent3, Set.of(0, 1, 5), Set.of(0, 1, 5)).isEmpty(), is(false));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent2, persistent3, Set.of(0, 1, 5), Set.of(0, 1, 5)).shrinkSize(), is(false));
    }

    @ParallelTest
    public void testSizeChanges()    {
        Storage persistent = new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build();
        Storage persistent2 = new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("1000Gi").build();
        Storage persistent3 = new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("10Gi").build();
        Storage persistent4 = new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("3.2Ti").build(); // Used to test millibytes
        Storage persistent5 = new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("3518437208883200m").build(); // Used to test millibytes

        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent, Set.of(0, 1, 5), Set.of(0, 1, 5)).shrinkSize(), is(false));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent2, Set.of(0, 1, 5), Set.of(0, 1, 5)).shrinkSize(), is(false));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent3, Set.of(0, 1, 5), Set.of(0, 1, 5)).shrinkSize(), is(true));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent4, persistent5, Set.of(0, 1, 5), Set.of(0, 1, 5)).shrinkSize(), is(false));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent4, persistent, Set.of(0, 1, 5), Set.of(0, 1, 5)).shrinkSize(), is(true));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent5, persistent, Set.of(0, 1, 5), Set.of(0, 1, 5)).shrinkSize(), is(true));
    }

    @ParallelTest
    public void testEphemeralDiff()    {
        Storage ephemeral = new EphemeralStorageBuilder().build();

        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, ephemeral, ephemeral, Set.of(0, 1, 5), Set.of(0, 1, 5)).changesType(), is(false));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, ephemeral, ephemeral, Set.of(0, 1, 5), Set.of(0, 1, 5)).isEmpty(), is(true));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, ephemeral, ephemeral, Set.of(0, 1, 5), Set.of(0, 1, 5)).shrinkSize(), is(false));
    }

    @ParallelTest
    public void testCrossDiff()    {
        Storage jbod = new JbodStorageBuilder().withVolumes(
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build(),
                new PersistentClaimStorageBuilder().withStorageClass("gp2-st1").withDeleteClaim(true).withId(1).withSize("1000Gi").build())
                .build();

        Storage ephemeral = new EphemeralStorageBuilder().build();

        Storage persistent = new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build();

        StorageDiff diffJbodEphemeral = new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, jbod, ephemeral, Set.of(0, 1, 5), Set.of(0, 1, 5));
        StorageDiff diffPersistentEphemeral = new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, ephemeral, Set.of(0, 1, 5), Set.of(0, 1, 5));
        StorageDiff diffJbodPersistent = new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, jbod, persistent, Set.of(0, 1, 5), Set.of(0, 1, 5));

        assertThat(diffJbodEphemeral.changesType(), is(true));
        assertThat(diffPersistentEphemeral.changesType(), is(true));
        assertThat(diffJbodPersistent.changesType(), is(true));

        assertThat(diffJbodEphemeral.isEmpty(), is(false));
        assertThat(diffPersistentEphemeral.isEmpty(), is(false));
        assertThat(diffJbodPersistent.isEmpty(), is(false));

        assertThat(diffJbodEphemeral.isVolumesAddedOrRemoved(), is(false));
        assertThat(diffPersistentEphemeral.isVolumesAddedOrRemoved(), is(false));
        assertThat(diffJbodPersistent.isVolumesAddedOrRemoved(), is(false));
    }

    @ParallelTest
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
        StorageDiff diff = new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, jbod, jbod2, Set.of(0, 1, 5), Set.of(0, 1, 5));
        assertThat(diff.changesType(), is(false));
        assertThat(diff.isEmpty(), is(true));
        assertThat(diff.shrinkSize(), is(false));
        assertThat(diff.isVolumesAddedOrRemoved(), is(true));

        // Volume removed
        diff = new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, jbod2, jbod, Set.of(0, 1, 5), Set.of(0, 1, 5));
        assertThat(diff.changesType(), is(false));
        assertThat(diff.isEmpty(), is(true));
        assertThat(diff.shrinkSize(), is(false));
        assertThat(diff.isVolumesAddedOrRemoved(), is(true));

        // Volume added with changes
        diff = new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, jbod, jbod3, Set.of(0, 1, 5), Set.of(0, 1, 5));
        assertThat(diff.changesType(), is(false));
        assertThat(diff.isEmpty(), is(false));
        assertThat(diff.shrinkSize(), is(true));
        assertThat(diff.isVolumesAddedOrRemoved(), is(true));

        // No volume added, but with changes
        diff = new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, jbod2, jbod3, Set.of(0, 1, 5), Set.of(0, 1, 5));
        assertThat(diff.changesType(), is(false));
        assertThat(diff.isEmpty(), is(false));
        assertThat(diff.shrinkSize(), is(true));
        assertThat(diff.isVolumesAddedOrRemoved(), is(false));

        // Volume removed from the beginning
        diff = new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, jbod3, jbod5, Set.of(0, 1, 5), Set.of(0, 1, 5));
        assertThat(diff.changesType(), is(false));
        assertThat(diff.isEmpty(), is(true));
        assertThat(diff.shrinkSize(), is(false));
        assertThat(diff.isVolumesAddedOrRemoved(), is(true));

        // Volume added to the beginning
        diff = new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, jbod5, jbod3, Set.of(0, 1, 5), Set.of(0, 1, 5));
        assertThat(diff.changesType(), is(false));
        assertThat(diff.isEmpty(), is(true));
        assertThat(diff.shrinkSize(), is(false));
        assertThat(diff.isVolumesAddedOrRemoved(), is(true));

        // Volume replaced with another ID and another volume which is kept changed
        diff = new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, jbod3, jbod6, Set.of(0, 1, 5), Set.of(0, 1, 5));
        assertThat(diff.changesType(), is(false));
        assertThat(diff.isEmpty(), is(false));
        assertThat(diff.shrinkSize(), is(false));
        assertThat(diff.isVolumesAddedOrRemoved(), is(true));

        // Volume replaced with another ID in single volume broker
        diff = new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, jbod, jbod4, Set.of(0, 1, 5), Set.of(0, 1, 5));
        assertThat(diff.changesType(), is(false));
        assertThat(diff.isEmpty(), is(true));
        assertThat(diff.shrinkSize(), is(false));
        assertThat(diff.isVolumesAddedOrRemoved(), is(true));

        // Volume replaced with another ID without changing the volumes which are kept
        diff = new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, jbod2, jbod6, Set.of(0, 1, 5), Set.of(0, 1, 5));
        assertThat(diff.changesType(), is(false));
        assertThat(diff.isEmpty(), is(true));
        assertThat(diff.shrinkSize(), is(false));
        assertThat(diff.isVolumesAddedOrRemoved(), is(true));
    }

    @ParallelTest
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

        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, jbod, jbod, Set.of(0, 1, 5), Set.of(0, 1, 5)).shrinkSize(), is(false));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, jbod, jbod2, Set.of(0, 1, 5), Set.of(0, 1, 5)).shrinkSize(), is(false));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, jbod, jbod3, Set.of(0, 1, 5), Set.of(0, 1, 5)).shrinkSize(), is(true));
    }

    @ParallelTest
    public void testPersistentDiffWithOverridesChangesToExistingOverrides()    {
        Storage persistent = new PersistentClaimStorageBuilder()
                .withStorageClass("gp2-ssd")
                .withDeleteClaim(false)
                .withId(0)
                .withSize("100Gi")
                .withOverrides(
                        new PersistentClaimStorageOverrideBuilder()
                                .withBroker(0)
                                .withStorageClass("gp2-ssd-az1")
                                .build(),
                        new PersistentClaimStorageOverrideBuilder()
                                .withBroker(3)
                                .withStorageClass("gp2-ssd-az2")
                                .build())
                .build();

        Storage persistent2 = new PersistentClaimStorageBuilder()
                .withStorageClass("gp2-ssd")
                .withDeleteClaim(false)
                .withId(0)
                .withSize("100Gi")
                .withOverrides(
                        new PersistentClaimStorageOverrideBuilder()
                                .withBroker(0)
                                .withStorageClass("gp2-ssd-az1")
                                .build(),
                        new PersistentClaimStorageOverrideBuilder()
                                .withBroker(3)
                                .withStorageClass("new-sc")
                                .build())
                .build();

        // Test no changes when the diff is the same
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent, Set.of(0, 3), Set.of(0, 3)).isEmpty(), is(true));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent2, persistent2, Set.of(0, 3), Set.of(0, 3)).isEmpty(), is(true));

        // Override changed for node which does not exist => is allowed
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent2, Set.of(0), Set.of(0)).isEmpty(), is(true));

        // Override changed for node which is being scaled up => is allowed
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent2, Set.of(0), Set.of(0, 3)).isEmpty(), is(true));

        // Override changed for existing node  => is not allowed
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent2, Set.of(0, 3), Set.of(0, 3)).isEmpty(), is(false));

        // Override changed for node being scaled down => is allowed
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent2, Set.of(0, 3), Set.of(0)).isEmpty(), is(true));
    }

    @ParallelTest
    public void testPersistentDiffWithOverridesBeingAdded()    {
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
                .withOverrides(
                        new PersistentClaimStorageOverrideBuilder()
                                .withBroker(0)
                                .withStorageClass("gp2-ssd-az1")
                                .build())
                .build();

        Storage persistent3 = new PersistentClaimStorageBuilder()
                .withStorageClass("gp2-ssd")
                .withDeleteClaim(false)
                .withId(0)
                .withSize("100Gi")
                .withOverrides(
                        new PersistentClaimStorageOverrideBuilder()
                                .withBroker(0)
                                .withStorageClass("gp2-ssd-az1")
                                .build(),
                        new PersistentClaimStorageOverrideBuilder()
                                .withBroker(3)
                                .withStorageClass("gp2-ssd-az2")
                                .build())
                .build();

        // Test no changes
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent, Set.of(0, 3), Set.of(0, 3)).isEmpty(), is(true));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent2, persistent2, Set.of(0, 3), Set.of(0, 3)).isEmpty(), is(true));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent3, persistent3, Set.of(0, 3), Set.of(0, 3)).isEmpty(), is(true));

        // Overrides added for existing nodes => not allowed
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent2, Set.of(0, 3), Set.of(0, 3)).isEmpty(), is(false));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent3, Set.of(0, 3), Set.of(0, 3)).isEmpty(), is(false));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent2, persistent3, Set.of(0, 3), Set.of(0, 3)).isEmpty(), is(false));

        // Overrides added for new nodes => allowed
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent2, persistent3, Set.of(0), Set.of(0, 3)).isEmpty(), is(true));

        // Overrides added for removed nodes => allowed
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent2, persistent3, Set.of(0, 3), Set.of(0)).isEmpty(), is(true));

        // Overrides added for non-existing nodes => allowed
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent2, persistent3, Set.of(0), Set.of(0)).isEmpty(), is(true));
    }

    @ParallelTest
    public void testPersistentDiffWithOverridesBeingRemoved()    {
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
                .withOverrides(
                        new PersistentClaimStorageOverrideBuilder()
                                .withBroker(0)
                                .withStorageClass("gp2-ssd-az1")
                                .build())
                .build();

        Storage persistent3 = new PersistentClaimStorageBuilder()
                .withStorageClass("gp2-ssd")
                .withDeleteClaim(false)
                .withId(0)
                .withSize("100Gi")
                .withOverrides(
                        new PersistentClaimStorageOverrideBuilder()
                                .withBroker(0)
                                .withStorageClass("gp2-ssd-az1")
                                .build(),
                        new PersistentClaimStorageOverrideBuilder()
                                .withBroker(3)
                                .withStorageClass("gp2-ssd-az2")
                                .build())
                .build();

        // Test no changes
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent, Set.of(0, 3), Set.of(0, 3)).isEmpty(), is(true));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent2, persistent2, Set.of(0, 3), Set.of(0, 3)).isEmpty(), is(true));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent3, persistent3, Set.of(0, 3), Set.of(0, 3)).isEmpty(), is(true));

        // Overrides removed for existing nodes => not allowed
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent3, persistent, Set.of(0, 3), Set.of(0, 3)).isEmpty(), is(false));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent3, persistent2, Set.of(0, 3), Set.of(0, 3)).isEmpty(), is(false));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent2, persistent, Set.of(0, 3), Set.of(0, 3)).isEmpty(), is(false));

        // Overrides removed for new nodes => allowed
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent3, persistent2, Set.of(0), Set.of(0, 3)).isEmpty(), is(true));

        // Overrides removed for removed nodes => allowed
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent3, persistent2, Set.of(0, 3), Set.of(0)).isEmpty(), is(true));

        // Overrides removed for non-existing nodes => allowed
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent3, persistent2, Set.of(0), Set.of(0)).isEmpty(), is(true));
    }

    @ParallelTest
    public void testPersistentDiffWithOverridesBeingAddedAndRemoved()    {
        Storage persistent = new PersistentClaimStorageBuilder()
                .withStorageClass("gp2-ssd")
                .withDeleteClaim(false)
                .withId(0)
                .withSize("100Gi")
                .withOverrides(
                        new PersistentClaimStorageOverrideBuilder()
                                .withBroker(0)
                                .withStorageClass("gp2-ssd-az1")
                                .build())
                .build();

        Storage persistent2 = new PersistentClaimStorageBuilder()
                .withStorageClass("gp2-ssd")
                .withDeleteClaim(false)
                .withId(0)
                .withSize("100Gi")
                .withOverrides(
                        new PersistentClaimStorageOverrideBuilder()
                                .withBroker(1)
                                .withStorageClass("gp2-ssd-az2")
                                .build())
                .build();

        // Test no changes
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent, Set.of(0, 3), Set.of(0, 3)).isEmpty(), is(true));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent2, persistent2, Set.of(0, 3), Set.of(0, 3)).isEmpty(), is(true));

        // Overrides added and removed for existing nodes => not allowed
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent2, persistent, Set.of(0, 3), Set.of(0, 3)).isEmpty(), is(false));
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent2, Set.of(0, 3), Set.of(0, 3)).isEmpty(), is(false));

        // Overrides added for new nodes but removed for old => not allowed
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent, persistent2, Set.of(0), Set.of(0, 3)).isEmpty(), is(false));

        // Overrides removed for new nodes but added for old => not allowed
        assertThat(new StorageDiff(Reconciliation.DUMMY_RECONCILIATION, persistent2, persistent, Set.of(0), Set.of(0, 3)).isEmpty(), is(false));
    }
}
