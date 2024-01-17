/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
public class AbstractModelTest {
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    // Implement AbstractModel to test the abstract class
    private static class Model extends AbstractModel   {
        public Model(HasMetadata resource) {
            super(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()),
                resource, resource.getMetadata().getName() + "-model-app", "model-app", SHARED_ENV_PROVIDER);
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
        JvmOptionUtils.jvmPerformanceOptions(envVars, am.jvmOptions);

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
}
