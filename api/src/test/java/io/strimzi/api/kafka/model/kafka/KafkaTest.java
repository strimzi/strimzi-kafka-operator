/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka;

import io.strimzi.api.kafka.model.common.TopologyLabelRack;
import io.strimzi.test.ReadWriteUtils;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Kafka-specific serialization checks that don't fit the parameterized round-trip in {@link
 * io.strimzi.api.kafka.model.CrdTest}.
 */
public class KafkaTest {
    /**
     * The type field was added to Rack awareness only later. That is why it is (unlike our other typed fields) optional.
     * This test checks that it is really optional and that it correctly deserializes into the correct type.
     */
    @Test
    public void testUntypedRackAwareness()    {
        Kafka model = ReadWriteUtils.readObjectFromYamlFileInResources("Kafka-untyped-topology-label-rack.yaml", Kafka.class);

        assertThat(model.getSpec().getKafka().getRack(), is(notNullValue()));
        assertThat(model.getSpec().getKafka().getRack(), is(instanceOf(TopologyLabelRack.class)));
        assertThat(((TopologyLabelRack) model.getSpec().getKafka().getRack()).getTopologyKey(), is("topology.kubernetes.io/zone"));
    }
}
