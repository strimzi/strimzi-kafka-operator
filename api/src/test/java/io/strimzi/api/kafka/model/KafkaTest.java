/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.KafkaListeners;
import io.strimzi.api.kafka.model.listener.KafkaListenersBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.ArrayOrObjectKafkaListeners;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.Collections;
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
    public void testOldListenerSerialization() throws URISyntaxException {
        KafkaListeners listeners = new KafkaListenersBuilder()
                .withNewTls()
                    .withAuth(new KafkaListenerAuthenticationTls())
                .endTls()
                .build();

        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName("my-cluster")
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withNewZookeeper()
                        .withReplicas(1)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                    .withNewKafka()
                        .withReplicas(1)
                        .withListeners(new ArrayOrObjectKafkaListeners(listeners))
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        assertThat(TestUtils.toYamlString(kafka), is(TestUtils.getFileAsString(this.getClass().getResource("Kafka-old-listener-serialization.yaml").toURI().getPath())));
    }

    @Test
    public void testCertificationAuthorityBuilderAndInts() throws URISyntaxException {
        List<GenericKafkaListener> listeners = Collections.singletonList(
                new GenericKafkaListenerBuilder()
                        .withName("lst")
                        .withPort(9092)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(true)
                        .build()
        );

        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName("my-cluster")
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withNewZookeeper()
                        .withReplicas(1)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                    .withNewKafka()
                        .withReplicas(1)
                        .withListeners(new ArrayOrObjectKafkaListeners(listeners))
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                    .withNewClientsCa()
                        .withGenerateSecretOwnerReference(false)
                    .endClientsCa()
                    .withNewClusterCa()
                        .withGenerateSecretOwnerReference(false)
                    .endClusterCa()
                .endSpec()
                .build();

        assertThat(TestUtils.toYamlString(kafka), is(TestUtils.getFileAsString(this.getClass().getResource("Kafka-ca-ints.yaml").toURI().getPath())));
    }

    @Test
    public void testNewListenerSerialization() throws URISyntaxException {
        List<GenericKafkaListener> listeners = Collections.singletonList(
                new GenericKafkaListenerBuilder()
                        .withName("lst")
                        .withPort(9092)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(true)
                .build()
        );

        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName("my-cluster")
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withNewZookeeper()
                        .withReplicas(1)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                    .withNewKafka()
                        .withReplicas(1)
                        .withListeners(new ArrayOrObjectKafkaListeners(listeners))
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        assertThat(TestUtils.toYamlString(kafka), is(TestUtils.getFileAsString(this.getClass().getResource("Kafka-new-listener-serialization.yaml").toURI().getPath())));
    }

    @Test
    public void testNewListeners()    {
        Kafka model = TestUtils.fromYaml("Kafka-with-array" + ".yaml", Kafka.class);

        assertThat(model.getSpec().getKafka().getListeners().getGenericKafkaListeners(), is(notNullValue()));
        assertThat(model.getSpec().getKafka().getListeners().getGenericKafkaListeners().size(), is(2));
        assertThat(model.getSpec().getKafka().getListeners().getKafkaListeners(), is(nullValue()));

        List<GenericKafkaListener> listeners = model.getSpec().getKafka().getListeners().getGenericKafkaListeners();

        assertThat(listeners.get(0).getAuth().getType(), is("scram-sha-512"));
        assertThat(listeners.get(1).getAuth().getType(), is("tls"));
    }

    @Test
    public void testOldListeners()    {
        Kafka model = TestUtils.fromYaml("Kafka" + ".yaml", Kafka.class);

        assertThat(model.getSpec().getKafka().getListeners().getGenericKafkaListeners(), is(nullValue()));
        assertThat(model.getSpec().getKafka().getListeners().getKafkaListeners(), is(notNullValue()));

        KafkaListeners listeners = model.getSpec().getKafka().getListeners().getKafkaListeners();

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
