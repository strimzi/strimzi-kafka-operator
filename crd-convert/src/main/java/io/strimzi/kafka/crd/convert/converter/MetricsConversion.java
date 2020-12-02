/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.converter;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import io.strimzi.api.kafka.model.MetricsConfig;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MetricsConversion<U extends HasMetadata> implements Conversion<U> {

    private static final ObjectMapper MAPPER = new YAMLMapper();
    private static final ThreadLocal<Map<String, String>> TL = new ThreadLocal<>();

    private Conversion<U> conversion;
    private Conversion<U> reverse;

    public <V extends HasConfigurableMetrics> MetricsConversion(String path, Class<V> type) {
        conversion = Conversion.replace(path, new MetricsInvertibleFunction<>(type));
        reverse = Conversion.replace(path, new InverseMetricsInvertibleFunction<>(type));
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
        private Class<T> clazz;

        public MetricsInvertibleFunction(Class<T> clazz) {
            this.clazz = clazz;
        }

        String get(T holder, String suffix) {
            String specName = TL.get().get("name");
            if (specName == null) {
                throw new IllegalStateException("Missing ObjectMeta::name on root resource!");
            }
            String holderType = holder.getTypeName();
            return String.format("%s-%s-jmx-exporter-configuration-%s", specName, holderType, suffix);
        }

        String getKey(T holder) {
            return get(holder, "key");
        }

        String getName(T holder) {
            return get(holder, "name");
        }

        @Override
        Class<T> convertedType() {
            return clazz;
        }

        @Override
        public InvertibleFunction<T> inverse() {
            return new InverseMetricsInvertibleFunction<>(convertedType());
        }

        @Override
        public T apply(T holder) {
            if (holder == null) {
                return null;
            }
            Map<String, Object> metrics = holder.getMetrics();
            if (metrics != null && holder.getMetricsConfig() == null) {
                String key = getKey(holder);
                String name = getName(holder);

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
                    (c, ns) -> c.configMaps().inNamespace(ns).withName(name).createOrReplace(configMap)
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
        public InverseMetricsInvertibleFunction(Class<T> clazz) {
            super(clazz);
        }

        @Override
        public InvertibleFunction<T> inverse() {
            return new MetricsInvertibleFunction<>(convertedType());
        }

        @Override
        public T apply(T holder) {
            if (holder == null) {
                return null;
            }
            MetricsConfig mc = holder.getMetricsConfig();
            // null is not an instance of JmxPrometheusExporterMetrics
            if (mc instanceof JmxPrometheusExporterMetrics) {
                JmxPrometheusExporterMetrics jmxMC = (JmxPrometheusExporterMetrics) mc;
                // TODO
            }
            return holder;
        }
    }

}

