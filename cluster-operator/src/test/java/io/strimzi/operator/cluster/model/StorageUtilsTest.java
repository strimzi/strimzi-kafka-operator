/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Quantity;
import io.strimzi.api.kafka.model.storage.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.storage.JbodStorageBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.storage.Storage;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class StorageUtilsTest {
    @Test
    public void testSizeConversion() {
        assertThat(StorageUtils.parseMemory("100Gi"), is(100L * 1_024L * 1_024L * 1_024L));
        assertThat(StorageUtils.parseMemory("100G"), is(100L * 1_000L * 1_000L * 1_000L));
        assertThat(StorageUtils.parseMemory("100000Mi"), is(100_000L * 1_024L * 1_024L));
        assertThat(StorageUtils.parseMemory("100000M"), is(100L * 1_000L * 1_000L * 1_000L));
        assertThat(StorageUtils.parseMemory("100Ti"), is(100L * 1_024L * 1_024L * 1_024L * 1_024L));
        assertThat(StorageUtils.parseMemory("100T"), is(100L * 1_000L * 1_000L * 1_000L * 1_000L));
        assertThat(StorageUtils.parseMemory("100Pi"), is(100L * 1_024L * 1_024L * 1_024L * 1_024L * 1_024L));
        assertThat(StorageUtils.parseMemory("100P"), is(100L * 1_000L * 1_000L * 1_000L * 1_000L * 1_000L));
        assertThat(StorageUtils.parseMemory("100.5P"), is((long) (100.5 * 1_000L * 1_000L * 1_000L * 1_000L * 1_000L)));
        assertThat(StorageUtils.parseMemory("2.1e6"), is((long) (2.1 * 1_000L * 1_000L)));

        assertThat(StorageUtils.parseMemory("100Gi") == StorageUtils.parseMemory("100Gi"), is(true));
        assertThat(StorageUtils.parseMemory("1000Gi") > StorageUtils.parseMemory("100Gi"), is(true));
        assertThat(StorageUtils.parseMemory("1000000Mi") > StorageUtils.parseMemory("100Gi"), is(true));
        assertThat(StorageUtils.parseMemory("100Pi") > StorageUtils.parseMemory("100Gi"), is(true));
        assertThat(StorageUtils.parseMemory("1000G") == StorageUtils.parseMemory("1T"), is(true));
    }

    @Test
    public void testQuantityConversion()    {
        assertThat(StorageUtils.parseMemory(new Quantity("1000G")), is(1_000L * 1_000L * 1_000L * 1_000L));
        assertThat(StorageUtils.parseMemory(new Quantity("100Gi")), is(100L * 1_024L * 1_024L * 1_024L));

        Quantity size = new Quantity("100", "Gi");
        assertThat(StorageUtils.parseMemory(size), is(100L * 1_024L * 1_024L * 1_024L));
    }

    @Test
    public void testEphemeralStorage() {
        Storage notEphemeral = new PersistentClaimStorageBuilder().build();
        Storage isEphemeral = new EphemeralStorageBuilder().build();
        Storage includesEphemeral = new JbodStorageBuilder().withVolumes(
                new EphemeralStorageBuilder().withId(1).build(),
                new EphemeralStorageBuilder().withId(2).build())
            .build();

        assertThat(StorageUtils.usesEphemeral(notEphemeral), is(false));
        assertThat(StorageUtils.usesEphemeral(isEphemeral), is(true));
        assertThat(StorageUtils.usesEphemeral(includesEphemeral), is(true));
    }
}
