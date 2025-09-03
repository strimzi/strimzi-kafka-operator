/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter.conversions;

import com.fasterxml.jackson.databind.JsonNode;
import io.strimzi.api.kafka.model.kafka.JbodStorage;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorage;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageOverrideBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@SuppressWarnings("deprecation")
class NodePoolConversionsTest extends AbstractConversionsTest {
    @Test
    public void testJbodOverridesRemoval() {
        KafkaNodePool nodePool = new KafkaNodePoolBuilder()
                .withNewSpec()
                    .withNewJbodStorage()
                        .addNewPersistentClaimStorageVolume()
                            .withId(0)
                            .withSize("100Gi")
                        .endPersistentClaimStorageVolume()
                        .addNewPersistentClaimStorageVolume()
                            .withId(1)
                            .withSize("100Gi")
                            .withOverrides(new PersistentClaimStorageOverrideBuilder().withBroker(3).withStorageClass("broker3-class").build())
                        .endPersistentClaimStorageVolume()
                    .endJbodStorage()
                .endSpec()
                .build();

        Conversion<KafkaNodePool> c = NodePoolConversions.storageOverrides();

        c.convert(nodePool);

        JbodStorage jbod = (JbodStorage) nodePool.getSpec().getStorage();
        jbod.getVolumes().forEach(v -> {
            PersistentClaimStorage volume = (PersistentClaimStorage) v;
            assertThat(volume.getOverrides(), is(nullValue()));
        });
    }

    @Test
    public void testJbodOverridesRemovalJson() {
        KafkaNodePool nodePool = new KafkaNodePoolBuilder()
                .withNewSpec()
                    .withNewJbodStorage()
                        .addNewPersistentClaimStorageVolume()
                            .withId(0)
                            .withSize("100Gi")
                        .endPersistentClaimStorageVolume()
                        .addNewPersistentClaimStorageVolume()
                            .withId(1)
                            .withSize("100Gi")
                            .withOverrides(new PersistentClaimStorageOverrideBuilder().withBroker(3).withStorageClass("broker3-class").build())
                        .endPersistentClaimStorageVolume()
                    .endJbodStorage()
                .endSpec()
                .build();
        JsonNode json = typedToJsonNode(nodePool);

        Conversion<KafkaNodePool> c = NodePoolConversions.storageOverrides();
        c.convert(json);

        JbodStorage jbod = (JbodStorage) jsonNodeToTyped(json, KafkaNodePool.class).getSpec().getStorage();
        jbod.getVolumes().forEach(v -> {
            PersistentClaimStorage volume = (PersistentClaimStorage) v;
            assertThat(volume.getOverrides(), is(nullValue()));
        });
    }

    @Test
    public void testOverridesRemoval() {
        KafkaNodePool nodePool = new KafkaNodePoolBuilder()
                .withNewSpec()
                    .withNewPersistentClaimStorage()
                        .withId(1)
                        .withSize("100Gi")
                        .withOverrides(new PersistentClaimStorageOverrideBuilder().withBroker(3).withStorageClass("broker3-class").build())
                    .endPersistentClaimStorage()
                .endSpec()
                .build();

        Conversion<KafkaNodePool> c = NodePoolConversions.storageOverrides();

        c.convert(nodePool);

        PersistentClaimStorage volume = (PersistentClaimStorage) nodePool.getSpec().getStorage();
        assertThat(volume.getOverrides(), is(nullValue()));
    }

    @Test
    public void testOverridesRemovalJson() {
        KafkaNodePool nodePool = new KafkaNodePoolBuilder()
                .withNewSpec()
                    .withNewPersistentClaimStorage()
                        .withId(1)
                        .withSize("100Gi")
                        .withOverrides(new PersistentClaimStorageOverrideBuilder().withBroker(3).withStorageClass("broker3-class").build())
                    .endPersistentClaimStorage()
                .endSpec()
                .build();
        JsonNode json = typedToJsonNode(nodePool);

        Conversion<KafkaNodePool> c = NodePoolConversions.storageOverrides();
        c.convert(json);

        PersistentClaimStorage volume = (PersistentClaimStorage) jsonNodeToTyped(json, KafkaNodePool.class).getSpec().getStorage();
        assertThat(volume.getOverrides(), is(nullValue()));
    }
}