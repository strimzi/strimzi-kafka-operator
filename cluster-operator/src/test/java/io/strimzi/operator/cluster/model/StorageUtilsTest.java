/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Quantity;
import io.strimzi.api.kafka.model.kafka.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.kafka.JbodStorage;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ParallelSuite
public class StorageUtilsTest {
    @ParallelTest
    public void testSizeConversion() {
        assertThat(StorageUtils.convertToMillibytes("100Gi"), is(100L * 1_024L * 1_024L * 1_024L * 1_000L));
        assertThat(StorageUtils.convertToMillibytes("100G"), is(100L * 1_000L * 1_000L * 1_000L * 1_000L));
        assertThat(StorageUtils.convertToMillibytes("100000Mi"), is(100_000L * 1_024L * 1_024L * 1_000L));
        assertThat(StorageUtils.convertToMillibytes("100000M"), is(100L * 1_000L * 1_000L * 1_000L * 1_000L));
        assertThat(StorageUtils.convertToMillibytes("100Ti"), is(100L * 1_024L * 1_024L * 1_024L * 1_024L * 1_000L));
        assertThat(StorageUtils.convertToMillibytes("100T"), is(100L * 1_000L * 1_000L * 1_000L * 1_000L * 1_000L));
        assertThat(StorageUtils.convertToMillibytes("1Pi"), is(1L * 1_024L * 1_024L * 1_024L * 1_024L * 1_024L * 1_000L));
        assertThat(StorageUtils.convertToMillibytes("1P"), is(1L * 1_000L * 1_000L * 1_000L * 1_000L * 1_000L * 1_000L));
        assertThat(StorageUtils.convertToMillibytes("1.5P"), is((long) (1.5 * 1_000L * 1_000L * 1_000L * 1_000L * 1_000L * 1_000L)));
        assertThat(StorageUtils.convertToMillibytes("2.1e6"), is((long) (2.1 * 1_000L * 1_000L * 1_000L)));
        assertThat(StorageUtils.convertToMillibytes("3.2Ti"), is(3_518_437_208_883_200L));
        assertThat(StorageUtils.convertToMillibytes("3518437208883200m"), is(3_518_437_208_883_200L));

        assertThat(StorageUtils.convertToMillibytes("100Gi") == StorageUtils.convertToMillibytes("100Gi"), is(true));
        assertThat(StorageUtils.convertToMillibytes("1000Gi") > StorageUtils.convertToMillibytes("100Gi"), is(true));
        assertThat(StorageUtils.convertToMillibytes("1000000Mi") > StorageUtils.convertToMillibytes("100Gi"), is(true));
        assertThat(StorageUtils.convertToMillibytes("3.2Ti") > StorageUtils.convertToMillibytes("3Ti"), is(true));
        assertThat(StorageUtils.convertToMillibytes("10Pi") > StorageUtils.convertToMillibytes("100Gi"), is(true));
        assertThat(StorageUtils.convertToMillibytes("1000G") == StorageUtils.convertToMillibytes("1T"), is(true));
        assertThat(StorageUtils.convertToMillibytes("3.2Ti") == StorageUtils.convertToMillibytes("3518437208883200m"), is(true));
    }

    @ParallelTest
    public void testQuantityConversion()    {
        assertThat(StorageUtils.convertToMillibytes(new Quantity("1000G")), is(1_000L * 1_000L * 1_000L * 1_000L * 1_000L));
        assertThat(StorageUtils.convertToMillibytes(new Quantity("100Gi")), is(100L * 1_024L * 1_024L * 1_024L * 1_000L));

        Quantity size = new Quantity("100", "Gi");
        assertThat(StorageUtils.convertToMillibytes(size), is(100L * 1_024L * 1_024L * 1_024L * 1_000L));
    }

    @ParallelTest
    public void testUnitConversions()    {
        assertThat(StorageUtils.convertTo("1000G", "M"), is(1_000_000.0));
        assertThat(StorageUtils.convertTo("1000Gi", "Mi"), is(1_024_000.0));
        assertThat(StorageUtils.convertTo("1000G", "Gi"), is(931.3225746154785));
        assertThat(StorageUtils.convertTo("3518437208883200m", "Ti"), is(3.2));
        assertThat(StorageUtils.convertTo("3.2Ti", "m"), is(3_518_437_208_883_200.0));
    }

    @ParallelTest
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

    @ParallelTest
    public void testStorageValidation() {
        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> StorageUtils.validatePersistentStorage(new JbodStorage(), "KafkaNodePool.spec.storage"));
        assertThat(e.getMessage(), is("JbodStorage needs to contain at least one volume (KafkaNodePool.spec.storage)"));

        e = assertThrows(InvalidResourceException.class, () -> StorageUtils.validatePersistentStorage(new JbodStorageBuilder().withVolumes(List.of()).build(), "KafkaNodePool.spec.storage"));
        assertThat(e.getMessage(), is("JbodStorage needs to contain at least one volume (KafkaNodePool.spec.storage)"));

        e = assertThrows(InvalidResourceException.class, () -> StorageUtils.validatePersistentStorage(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).build()).build(), "KafkaNodePool.spec.storage"));
        assertThat(e.getMessage(), is("The size is mandatory for a persistent-claim storage (KafkaNodePool.spec.storage)"));

        assertDoesNotThrow(() -> StorageUtils.validatePersistentStorage(new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build(), "KafkaNodePool.spec.storage"));
    }
}
