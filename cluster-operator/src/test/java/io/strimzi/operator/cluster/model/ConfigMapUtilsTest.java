/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ManagedFieldsEntryBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.strimzi.api.kafka.model.common.ExternalLoggingBuilder;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectSpecBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.logging.LoggingModel;
import io.strimzi.operator.cluster.model.logging.SupportsLogging;
import io.strimzi.operator.cluster.model.metrics.JmxPrometheusExporterModel;
import io.strimzi.operator.cluster.model.metrics.SupportsMetrics;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConfigMapUtilsTest {
    private final static String NAME = "my-cm";
    private final static String NAMESPACE = "my-namespace";
    private static final Labels LABELS = Labels
            .forStrimziKind("my-kind")
            .withStrimziName("my-name")
            .withStrimziCluster("my-cluster")
            .withStrimziComponentType("my-component-type")
            .withAdditionalLabels(Map.of("label-1", "value-1", "label-2", "value-2"));
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    @Test
    public void testConfigMapCreation() {
        ConfigMap cm = ConfigMapUtils.createConfigMap(NAME, NAMESPACE, LABELS, ResourceUtils.DUMMY_OWNER_REFERENCE, Map.of("key1", "value1", "key2", "value2"));

        assertThat(cm.getMetadata().getName(), is(NAME));
        assertThat(cm.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(cm.getMetadata().getOwnerReferences(), is(List.of(ResourceUtils.DUMMY_OWNER_REFERENCE)));
        assertThat(cm.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(cm.getMetadata().getAnnotations(), is(Map.of()));

        assertThat(cm.getData(), is(Map.of("key1", "value1", "key2", "value2")));
    }


    @Test
    public void testConfigMapDataNoMetricsNoLogging()   {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                .endMetadata()
                .build();

        Map<String, String> data = ConfigMapUtils.generateMetricsAndLogConfigMapData(Reconciliation.DUMMY_RECONCILIATION, new ModelWithoutMetricsAndLogging(kafka), new MetricsAndLogging(null, null));

        assertThat(data.size(), is(0));
    }

    @Test
    public void testConfigMapDataWithMetricsAndLogging()   {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                .endMetadata()
                .build();

        Map<String, String> data = ConfigMapUtils.generateMetricsAndLogConfigMapData(Reconciliation.DUMMY_RECONCILIATION, new ModelWithMetricsAndLogging(kafka), new MetricsAndLogging(new ConfigMapBuilder().withNewMetadata().withName("metrics-cm").endMetadata().withData(Map.of("metrics.yaml", "")).build(), new ConfigMapBuilder().withNewMetadata().withName("logging-cm").endMetadata().withData(Map.of("log4j.properties", "")).build()));

        assertThat(data.size(), is(2));
        assertThat(data.get(JmxPrometheusExporterModel.CONFIG_MAP_KEY), is(notNullValue()));
        assertThat(data.get(LoggingModel.LOG4J2_CONFIG_MAP_KEY), is(notNullValue()));
    }

    @Test
    public void testCreateFromExistingConfigMap() {
        Map<String, String> existingData = Map.of("key1", "value1", "key2", "value2");
        ConfigMap existingConfigMap = ConfigMapUtils.createConfigMap(NAME, NAMESPACE, LABELS, ResourceUtils.DUMMY_OWNER_REFERENCE, existingData);
        existingConfigMap.getMetadata().setManagedFields(List.of(new ManagedFieldsEntryBuilder().withManager("my-manager").build()));

        ConfigMap newFromExisting = ConfigMapUtils.createFromExistingConfigMap(existingConfigMap, null, null, null);

        assertThat(newFromExisting.getMetadata().getManagedFields(), nullValue());
        assertThat(newFromExisting.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(newFromExisting.getMetadata().getOwnerReferences(), is(List.of(ResourceUtils.DUMMY_OWNER_REFERENCE)));
        assertThat(newFromExisting.getData(), is(existingData));

        Labels additionalLabels = Labels.fromMap(Map.of("another-label", "value"));
        Map<String, String> additionalData = Map.of("key3", "value3");
        OwnerReference differentOwnerReference = new OwnerReferenceBuilder().withName("my-different-reference").build();

        newFromExisting = ConfigMapUtils.createFromExistingConfigMap(existingConfigMap, additionalLabels, additionalData, differentOwnerReference);

        Map<String, String> expectedLabels = new HashMap<>(LABELS.toMap());
        expectedLabels.putAll(additionalLabels.toMap());

        Map<String, String> expectedData = new HashMap<>(existingData);
        expectedData.putAll(additionalData);

        assertThat(newFromExisting.getMetadata().getManagedFields(), nullValue());
        assertThat(newFromExisting.getMetadata().getLabels(), is(expectedLabels));
        assertThat(newFromExisting.getMetadata().getOwnerReferences(), is(List.of(ResourceUtils.DUMMY_OWNER_REFERENCE)));
        assertThat(newFromExisting.getData(), is(expectedData));

        // set labels, OwnerReference, and data to `null` to check if they will be correctly set
        existingConfigMap.getMetadata().setOwnerReferences(null);
        existingConfigMap.getMetadata().setLabels(null);
        existingConfigMap.setData(null);

        newFromExisting = ConfigMapUtils.createFromExistingConfigMap(existingConfigMap, additionalLabels, additionalData, differentOwnerReference);

        assertThat(newFromExisting.getMetadata().getManagedFields(), nullValue());
        assertThat(newFromExisting.getMetadata().getLabels(), is(additionalLabels.toMap()));
        assertThat(newFromExisting.getMetadata().getOwnerReferences(), is(List.of(differentOwnerReference)));
        assertThat(newFromExisting.getData(), is(additionalData));
    }

    private static class ModelWithoutMetricsAndLogging extends AbstractModel   {
        public ModelWithoutMetricsAndLogging(HasMetadata resource) {
            super(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()),
                resource, resource.getMetadata().getName() + "-model-app", "model-app", SHARED_ENV_PROVIDER);
        }
    }

    private static class ModelWithMetricsAndLogging extends AbstractModel implements SupportsMetrics, SupportsLogging {
        public ModelWithMetricsAndLogging(HasMetadata resource) {
            super(new Reconciliation("test", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()),
                resource, resource.getMetadata().getName() + "-model-app", "model-app", SHARED_ENV_PROVIDER);
        }

        @Override
        public LoggingModel logging() {
            return new LoggingModel(new KafkaConnectSpecBuilder().withLogging(new ExternalLoggingBuilder().withNewValueFrom().withConfigMapKeyRef(new ConfigMapKeySelector("log4j.properties", "logging-cm", false)).endValueFrom().build()).build(), "KafkaConnectCluster");
        }

        @Override
        public JmxPrometheusExporterModel metrics() {
            return new JmxPrometheusExporterModel(new KafkaConnectSpecBuilder().withMetricsConfig(new JmxPrometheusExporterMetricsBuilder().withNewValueFrom().withConfigMapKeyRef(new ConfigMapKeySelector("metrics.yaml", "metrics-cm", false)).endValueFrom().build()).build());
        }
    }
}
