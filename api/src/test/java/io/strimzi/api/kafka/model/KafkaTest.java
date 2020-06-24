/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.strimzi.api.kafka.model.listener.KafkaListeners;
import io.strimzi.api.kafka.model.listener.v2.GenericKafkaListener;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * The purpose of this test is to ensure:
 *
 * 1. we get a correct tree of POJOs when reading a JSON/YAML `Kafka` resource.
 */
public class KafkaTest extends AbstractCrdTest<Kafka> {

    public KafkaTest() {
        super(Kafka.class);
    }

    @Test
    public void testArrayRoundTrip()    {
        rt("Kafka-with-array");
    }

    @Test
    public void testNewListeners()    {
        Kafka model = TestUtils.fromYaml("Kafka-with-array" + ".yaml", Kafka.class);

        assertThat(model.getSpec().getKafka().getListeners().getListValue(), is(notNullValue()));
        assertThat(model.getSpec().getKafka().getListeners().getListValue().size(), is(2));
        assertThat(model.getSpec().getKafka().getListeners().getObjectValue(), is(nullValue()));

        List<GenericKafkaListener> listeners = model.getSpec().getKafka().getListeners().getListValue();

        assertThat(listeners.get(0).getAuth().getType(), is("scram-sha-512"));
        assertThat(listeners.get(1).getAuth().getType(), is("tls"));
    }

    @Test
    public void testOldListeners()    {
        Kafka model = TestUtils.fromYaml("Kafka" + ".yaml", Kafka.class);

        assertThat(model.getSpec().getKafka().getListeners().getListValue(), is(nullValue()));
        assertThat(model.getSpec().getKafka().getListeners().getObjectValue(), is(notNullValue()));

        KafkaListeners listeners = model.getSpec().getKafka().getListeners().getObjectValue();

        assertThat(listeners.getPlain(), is(notNullValue()));
        assertThat(listeners.getTls(), is(notNullValue()));
        assertThat(listeners.getPlain().getAuth().getType(), is("scram-sha-512"));
        assertThat(listeners.getTls().getAuth().getType(), is("tls"));
    }

    public void rt(String resourceName) {
        Kafka model = TestUtils.fromYaml(resourceName + ".yaml", Kafka.class);
        assertThat("The classpath resource " + resourceName + " does not exist", model, is(notNullValue()));

        ObjectMeta metadata = model.getMetadata();
        assertThat(metadata, is(notNullValue()));
        assertDesiredResource(model, resourceName + ".out.yaml");
        assertDesiredResource(TestUtils.fromYamlString(TestUtils.toYamlString(model), Kafka.class), resourceName + ".out.yaml");
    }
}
