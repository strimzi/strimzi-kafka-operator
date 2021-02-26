/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.converter;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.strimzi.api.kafka.model.ExternalConfigurationReference;
import io.strimzi.api.kafka.model.HasConfigurableMetrics;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MetricsConversion<U extends HasMetadata> implements Conversion<U> {
    private static final ObjectMapper MAPPER = new YAMLMapper(new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));
    private static final ThreadLocal<Map<String, String>> TL = new ThreadLocal<>();

    private final Conversion<U> conversion;
    private final Conversion<U> reverse;

    public <V extends HasConfigurableMetrics> MetricsConversion(String path, Class<V> type, String holderType) {
        conversion = Conversion.replace(path, new MetricsInvertibleFunction<>(type, holderType));
        reverse = Conversion.replace(path, new InverseMetricsInvertibleFunction<>(type, holderType));
    }

    @Override
    public void convert(JsonNode node) {
        Map<String, String> meta = new HashMap<>();
        JsonNode om = node.get("metadata");
        if (om != null) {
            JsonNode name = om.get("name");
            if (name != null) {
                meta.put("name", name.asText());
            }
        }
        meta.put("kind", node.get("kind").asText());
        TL.set(meta);
        try {
            conversion.convert(node);
        } catch (Exception e) {
            TL.remove();
        }
    }

    @Override
    public void convert(U instance) {
        Map<String, String> meta = new HashMap<>();
        ObjectMeta om = instance.getMetadata();
        if (om != null) {
            meta.put("name", om.getName());
        }
        meta.put("kind", instance.getKind());
        TL.set(meta);
        try {
            conversion.convert(instance);
        } catch (Exception e) {
            TL.remove();
        }
    }

    static class MetricsInvertibleFunction<T extends HasConfigurableMetrics> extends DefaultInvertibleFunction<T> {
        protected final String holderType;
        private final Class<T> clazz;

        public MetricsInvertibleFunction(Class<T> clazz, String holderType) {
            this.clazz = clazz;
            this.holderType = holderType;
        }

        String get(String suffix) {
            String specName = TL.get().get("name");
            if (specName == null) {
                throw new IllegalStateException("Missing ObjectMeta::name on root resource!");
            }
            return String.format("%s-%s-jmx-exporter-configuration%s", specName, holderType, suffix);
        }

        String getKey() {
            return get(".yaml");
        }

        String getName() {
            return get("");
        }

        @Override
        Class<T> convertedType() {
            return clazz;
        }

        @Override
        public InvertibleFunction<T> inverse() {
            return new InverseMetricsInvertibleFunction<>(convertedType(), holderType);
        }

        @Override
        public T apply(T holder) {
            if (holder == null) {
                return null;
            }
            Map<String, Object> metrics = holder.getMetrics();
            if (metrics != null && holder.getMetricsConfig() == null) {
                String key = getKey();
                String name = getName();

                JmxPrometheusExporterMetrics mc = new JmxPrometheusExporterMetrics();
                ExternalConfigurationReference valueFrom = new ExternalConfigurationReference();
                valueFrom.setConfigMapKeyRef(new ConfigMapKeySelector(key, name, null));
                mc.setValueFrom(valueFrom);
                holder.setMetricsConfig(mc);

                StringWriter writer = new StringWriter();
                try {
                    MAPPER.writerFor(Map.class).with(new DefaultPrettyPrinter()).writeValue(writer, metrics);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                String metricsAsYaml = writer.toString();

                ConfigMap configMap = new ConfigMapBuilder()
                    .withMetadata(new ObjectMetaBuilder().withName(name).build())
                    .withData(Collections.singletonMap(key, metricsAsYaml))
                    .build();

                MultipartConversions.get().addLast(new MultipartResource(
                    name,
                    configMap,
                    (c, namespace) -> c.configMaps().inNamespace(namespace).createOrReplace(configMap)
                ));

                holder.setMetrics(null); // clear old metrics
            }
            return holder;
        }
    }

    @Override
    public Conversion<U> reverse() {
        return reverse;
    }

    static class InverseMetricsInvertibleFunction<T extends HasConfigurableMetrics> extends MetricsInvertibleFunction<T> {
        public InverseMetricsInvertibleFunction(Class<T> clazz, String holderType) {
            super(clazz, holderType);
        }

        @Override
        public InvertibleFunction<T> inverse() {
            return new MetricsInvertibleFunction<>(convertedType(), holderType);
        }

        @Override
        public T apply(T holder) {
            if (holder == null) {
                return null;
            }

            throw new UnsupportedOperationException("Reverse conversion of metrics is not supported");
        }
    }

}

