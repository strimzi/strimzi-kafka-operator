/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.JvmOptions;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.TestUtils;

import io.fabric8.kubernetes.api.model.OwnerReference;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class AbstractModelTest {

    private static JvmOptions jvmOptions(String xmx, String xms) {
        JvmOptions result = new JvmOptions();
        result.setXms(xms);
        result.setXmx(xmx);
        return result;
    }

    @Test
    public void testJvmMemoryOptionsExplicit() {
        Map<String, String> env = getStringStringMap("4", "4",
                0.5, 4_000_000_000L, null);
        assertThat(env.get(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS), is("-Xms4 -Xmx4"));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_FRACTION), is(nullValue()));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_MAX), is(nullValue()));
    }

    private Map<String, String> getStringStringMap(String xmx, String xms, double dynamicFraction, long dynamicMax, ResourceRequirements resources) {
        Kafka resource = new KafkaBuilder()
                .withNewMetadata()
                .endMetadata()
                .build();
        AbstractModel am = new AbstractModel(resource, "") {
            @Override
            protected String getDefaultLogConfigFileName() {
                return "";
            }

            @Override
            protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {
                return emptyList();
            }
        };

        am.setLabels(Labels.forStrimziCluster("foo"));
        am.setJvmOptions(jvmOptions(xmx, xms));
        am.setResources(resources);
        List<EnvVar> envVars = new ArrayList<>(1);
        am.heapOptions(envVars, dynamicFraction, dynamicMax);
        return envVars.stream().collect(Collectors.toMap(e -> e.getName(), e -> e.getValue()));
    }

    @Test
    public void testJvmMemoryOptionsXmsOnly() {
        Map<String, String> env = getStringStringMap(null, "4",
                0.5, 5_000_000_000L, null);
        assertThat(env.get(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS), is("-Xms4"));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_FRACTION), is(nullValue()));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_MAX), is(nullValue()));
    }

    @Test
    public void testJvmMemoryOptionsXmxOnly() {
        Map<String, String> env = getStringStringMap("4", null,
                0.5, 5_000_000_000L, null);
        assertThat(env.get(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS), is("-Xmx4"));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_FRACTION), is(nullValue()));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_MAX), is(nullValue()));
    }


    @Test
    public void testJvmMemoryOptionsDefaultWithNoMemoryLimitOrJvmOptions() {
        Map<String, String> env = getStringStringMap(null, null,
                0.5, 5_000_000_000L, null);
        assertThat(env.get(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS), is("-Xms" + AbstractModel.DEFAULT_JVM_XMS));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_FRACTION), is(nullValue()));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_MAX), is(nullValue()));
    }

    private ResourceRequirements getResourceRequest() {
        return new ResourceRequirementsBuilder()
                .addToRequests("memory", new Quantity("16000000000")).build();
    }

    @Test
    public void testJvmMemoryOptionsDefaultWithMemoryLimit() {
        Map<String, String> env = getStringStringMap(null, "4",
                0.5, 5_000_000_000L, getResourceRequest());
        assertThat(env.get(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS), is("-Xms4"));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_FRACTION), is("0.5"));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_MAX), is("5000000000"));
    }

    @Test
    public void testJvmMemoryOptionsMemoryRequest() {
        Map<String, String> env = getStringStringMap(null, null,
                0.7, 10_000_000_000L, getResourceRequest());
        assertThat(env.get(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS), is(nullValue()));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_FRACTION), is("0.7"));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_MAX), is("10000000000"));
    }

    @Test
    public void testJvmPerformanceOptions() {
        JvmOptions opts = TestUtils.fromJson("{}", JvmOptions.class);

        assertThat(getPerformanceOptions(opts), is(nullValue()));

        opts = TestUtils.fromJson("{" +
                "  \"-server\": \"true\"" +
                "}", JvmOptions.class);

        assertThat(getPerformanceOptions(opts), is("-server"));

        opts = TestUtils.fromJson("{" +
                "    \"-XX\":" +
                "            {\"key1\": \"value1\"," +
                "            \"key2\": \"true\"," +
                "            \"key3\": false," +
                "            \"key4\": 10}" +
                "}", JvmOptions.class);

        assertThat(getPerformanceOptions(opts), is("-XX:key1=value1 -XX:+key2 -XX:-key3 -XX:key4=10"));
    }

    private String getPerformanceOptions(JvmOptions opts) {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                .endMetadata()
                .build();

        AbstractModel am = new AbstractModel(kafka, "") {
            @Override
            protected String getDefaultLogConfigFileName() {
                return "";
            }

            @Override
            protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {
                return emptyList();
            }
        };

        am.setLabels(Labels.forStrimziCluster("foo"));
        am.setJvmOptions(opts);
        List<EnvVar> envVars = new ArrayList<>(1);
        am.jvmPerformanceOptions(envVars);

        if (!envVars.isEmpty()) {
            return envVars.get(0).getValue();
        } else {
            return null;
        }
    }

    @Test
    public void testOwnerReference() {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName("my-cluster")
                    .withNamespace("my-namespace")
                .endMetadata()
                .build();

        AbstractModel am = new AbstractModel(kafka, "my-app") {
            @Override
            protected String getDefaultLogConfigFileName() {
                return "";
            }

            @Override
            protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {
                return emptyList();
            }
        };
        am.setLabels(Labels.forStrimziCluster("foo"));
        am.setOwnerReference(kafka);

        OwnerReference ref = am.createOwnerReference();

        assertThat(ref.getApiVersion(), is(kafka.getApiVersion()));
        assertThat(ref.getKind(), is(kafka.getKind()));
        assertThat(ref.getName(), is(kafka.getMetadata().getName()));
        assertThat(ref.getUid(), is(kafka.getMetadata().getUid()));
    }

    @Test
    public void testDetermineImagePullPolicy()  {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName("my-cluster")
                    .withNamespace("my-namespace")
                .endMetadata()
                .build();

        AbstractModel am = new AbstractModel(kafka, "my-app") {
            @Override
            protected String getDefaultLogConfigFileName() {
                return "";
            }

            @Override
            protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {
                return emptyList();
            }
        };
        am.setLabels(Labels.forStrimziCluster("foo"));

        assertThat(am.determineImagePullPolicy(ImagePullPolicy.ALWAYS, "docker.io/repo/image:tag"), is(ImagePullPolicy.ALWAYS.toString()));
        assertThat(am.determineImagePullPolicy(ImagePullPolicy.IFNOTPRESENT, "docker.io/repo/image:tag"), is(ImagePullPolicy.IFNOTPRESENT.toString()));
        assertThat(am.determineImagePullPolicy(ImagePullPolicy.IFNOTPRESENT, "docker.io/repo/image:latest"), is(ImagePullPolicy.IFNOTPRESENT.toString()));
        assertThat(am.determineImagePullPolicy(ImagePullPolicy.NEVER, "docker.io/repo/image:tag"), is(ImagePullPolicy.NEVER.toString()));
        assertThat(am.determineImagePullPolicy(ImagePullPolicy.NEVER, "docker.io/repo/image:latest-kafka-2.6.0"), is(ImagePullPolicy.NEVER.toString()));
        assertThat(am.determineImagePullPolicy(null, "docker.io/repo/image:latest"), is(ImagePullPolicy.ALWAYS.toString()));
        assertThat(am.determineImagePullPolicy(null, "docker.io/repo/image:not-so-latest"), is(ImagePullPolicy.IFNOTPRESENT.toString()));
        assertThat(am.determineImagePullPolicy(null, "docker.io/repo/image:latest-kafka-2.6.0"), is(ImagePullPolicy.ALWAYS.toString()));
    }
}
