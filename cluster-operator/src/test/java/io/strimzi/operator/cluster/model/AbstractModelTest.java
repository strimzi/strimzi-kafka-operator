/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.strimzi.api.kafka.model.JvmOptions;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.storage.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.storage.JbodStorageBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
public class AbstractModelTest {

    // Implement AbstractModel to test the abstract class
    private static class Model extends AbstractModel   {
        public Model(HasMetadata resource) {
            super(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()), resource, resource.getMetadata().getName() + "-model-app", "model-app");
        }

    }

    @ParallelTest
    public void testJvmPerformanceOptions() {
        JvmOptions opts = TestUtils.fromYamlString("{}", JvmOptions.class);

        assertThat(getPerformanceOptions(opts), is(nullValue()));

        opts = TestUtils.fromYamlString("-XX:\n" +
                                        "  key1: value1\n" +
                                        "  key2: true\n" +
                                        "  key3: false\n" +
                                        "  key4: 10\n",
                JvmOptions.class);

        assertThat(getPerformanceOptions(opts), is("-XX:key1=value1 -XX:+key2 -XX:-key3 -XX:key4=10"));
    }

    private String getPerformanceOptions(JvmOptions opts) {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                .endMetadata()
                .build();

        AbstractModel am = new Model(kafka);
        am.jvmOptions = opts;
        List<EnvVar> envVars = new ArrayList<>(1);
        ModelUtils.jvmPerformanceOptions(envVars, am.jvmOptions);

        if (!envVars.isEmpty()) {
            return envVars.get(0).getValue();
        } else {
            return null;
        }
    }

    @ParallelTest
    public void testOwnerReference() {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName("my-cluster")
                    .withNamespace("my-namespace")
                    .withUid("6d92db8a-a1f9-4c18-a663-d88731bd52bb")
                .endMetadata()
                .build();

        AbstractModel am = new Model(kafka);

        OwnerReference ref = am.ownerReference;

        assertThat(ref.getApiVersion(), is(kafka.getApiVersion()));
        assertThat(ref.getKind(), is(kafka.getKind()));
        assertThat(ref.getName(), is(kafka.getMetadata().getName()));
        assertThat(ref.getUid(), is(kafka.getMetadata().getUid()));
    }

    @ParallelTest
    public void testCreatePersistentVolumeClaims()    {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName("my-cluster")
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withListeners(new GenericKafkaListenerBuilder().withName("plain").withPort(9092).withTls(false).withType(KafkaListenerType.INTERNAL).build())
                        .withReplicas(2)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                .endSpec()
                .build();

        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, KafkaVersionTestUtils.getKafkaVersionLookup());

        // JBOD Storage
        Storage storage = new JbodStorageBuilder().withVolumes(
                        new PersistentClaimStorageBuilder()
                                .withDeleteClaim(false)
                                .withId(0)
                                .withSize("20Gi")
                                .build(),
                        new PersistentClaimStorageBuilder()
                                .withDeleteClaim(true)
                                .withId(1)
                                .withSize("10Gi")
                                .build())
                .build();

        List<PersistentVolumeClaim> pvcs = kc.generatePersistentVolumeClaims(storage);

        assertThat(pvcs.size(), is(4));
        assertThat(pvcs.get(0).getMetadata().getName(), is("data-0-my-cluster-kafka-0"));
        assertThat(pvcs.get(1).getMetadata().getName(), is("data-0-my-cluster-kafka-1"));
        assertThat(pvcs.get(2).getMetadata().getName(), is("data-1-my-cluster-kafka-0"));
        assertThat(pvcs.get(3).getMetadata().getName(), is("data-1-my-cluster-kafka-1"));

        // JBOD with Ephemeral storage
        storage = new JbodStorageBuilder().withVolumes(
                        new PersistentClaimStorageBuilder()
                                .withDeleteClaim(false)
                                .withId(0)
                                .withSize("20Gi")
                                .build(),
                        new EphemeralStorageBuilder()
                                .withId(1)
                                .build())
                .build();

        pvcs = kc.generatePersistentVolumeClaims(storage);

        assertThat(pvcs.size(), is(2));
        assertThat(pvcs.get(0).getMetadata().getName(), is("data-0-my-cluster-kafka-0"));
        assertThat(pvcs.get(1).getMetadata().getName(), is("data-0-my-cluster-kafka-1"));

        // Persistent Claim storage
        storage = new PersistentClaimStorageBuilder()
                .withDeleteClaim(false)
                .withSize("20Gi")
                .build();

        pvcs = kc.generatePersistentVolumeClaims(storage);

        assertThat(pvcs.size(), is(2));
        assertThat(pvcs.get(0).getMetadata().getName(), is("data-my-cluster-kafka-0"));
        assertThat(pvcs.get(1).getMetadata().getName(), is("data-my-cluster-kafka-1"));

        // Persistent Claim with ID storage
        storage = new PersistentClaimStorageBuilder()
                .withDeleteClaim(false)
                .withId(0)
                .withSize("20Gi")
                .build();

        pvcs = kc.generatePersistentVolumeClaims(storage);

        assertThat(pvcs.size(), is(2));
        assertThat(pvcs.get(0).getMetadata().getName(), is("data-my-cluster-kafka-0"));
        assertThat(pvcs.get(1).getMetadata().getName(), is("data-my-cluster-kafka-1"));

        // Ephemeral Storage
        storage = new EphemeralStorageBuilder().build();

        pvcs = kc.generatePersistentVolumeClaims(storage);

        assertThat(pvcs.size(), is(0));

        // JBOD Storage without ID
        final Storage finalStorage = new JbodStorageBuilder().withVolumes(
                        new PersistentClaimStorageBuilder()
                                .withDeleteClaim(false)
                                .withSize("20Gi")
                                .build())
                .build();

        InvalidResourceException ex = Assertions.assertThrows(
                InvalidResourceException.class,
                () -> kc.generatePersistentVolumeClaims(finalStorage)
        );

        assertThat(ex.getMessage(), is("The 'id' property is required for volumes in JBOD storage."));
    }
}
