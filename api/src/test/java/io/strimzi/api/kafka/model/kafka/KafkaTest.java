/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.strimzi.api.kafka.model.AbstractCrdTest;
import io.strimzi.api.kafka.model.common.ConditionBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.kafka.listener.ListenerAddressBuilder;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatus;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatusBuilder;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
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
                        .withListeners(listeners)
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

        String path = Objects.requireNonNull(this.getClass().getResource("Kafka-ca-ints.yaml")).toURI().getPath();
        assertThat(TestUtils.toYamlString(kafka), is(TestUtils.getFileAsString(path)));
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
                        .withListeners(listeners)
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

        String path = Objects.requireNonNull(this.getClass().getResource("Kafka-new-listener-serialization.yaml")).toURI().getPath();
        assertThat(TestUtils.toYamlString(kafka), is(TestUtils.getFileAsString(path)));
    }

    @Test
    public void testListeners()    {
        Kafka model = TestUtils.fromYaml("Kafka" + ".yaml", Kafka.class);

        assertThat(model.getSpec().getKafka().getListeners(), is(notNullValue()));
        assertThat(model.getSpec().getKafka().getListeners().size(), is(2));

        List<GenericKafkaListener> listeners = model.getSpec().getKafka().getListeners();

        assertThat(listeners.get(0).getAuth().getType(), is("scram-sha-512"));
        assertThat(listeners.get(1).getAuth().getType(), is("tls"));
    }

    @Test
    public void testListenerTypeAndNameInStatus() throws ParseException, URISyntaxException {

        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName("my-cluster")
                    .withNamespace("my-namespace")
                    .withGeneration(2L)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withListeners(new GenericKafkaListenerBuilder()
                            .withName("plain")
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .build())
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewZookeeper()
                         .withReplicas(3)
                         .withNewEphemeralStorage()
                         .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .withNewStatus()
                    .withObservedGeneration(1L)
                    .withConditions(new ConditionBuilder()
                            .withType("Ready")
                            .withStatus("True")
                            .build())
                    .withListeners(new ListenerStatusBuilder()
                                   .withName("plain")
                                   .withAddresses(new ListenerAddressBuilder()
                                           .withHost("my-service.my-namespace.svc")
                                           .withPort(9092)
                                           .build())
                                   .build(),
                        new ListenerStatusBuilder()
                                   .withName("external")
                                   .withAddresses(new ListenerAddressBuilder()
                                           .withHost("my-route-address.domain.tld")
                                           .withPort(443)
                                           .build())
                                   .build())
                .endStatus()
                .build();

        String path = Objects.requireNonNull(this.getClass().getResource("Kafka-listener-name-and-status.yaml")).toURI().getPath();
        assertThat(TestUtils.toYamlString(kafka), is(TestUtils.getFileAsString(path)));
    }

    @Test
    public void testListenersTypeAndName()    {
        Kafka model = TestUtils.fromYaml("Kafka-listener-name-and-status" + ".yaml", Kafka.class);

        assertThat(model.getStatus().getListeners(), is(notNullValue()));
        assertThat(model.getStatus().getListeners().size(), is(2));

        List<ListenerStatus> listeners = model.getStatus().getListeners();

        assertThat(listeners.get(0).getName(), is("plain"));
        assertThat(listeners.get(1).getName(), is("external"));
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
