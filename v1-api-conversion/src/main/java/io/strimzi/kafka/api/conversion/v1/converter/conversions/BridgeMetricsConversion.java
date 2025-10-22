/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter.conversions;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.dsl.Updatable;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeSpec;
import io.strimzi.api.kafka.model.common.ExternalConfigurationReference;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetrics;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Converter for converting Bridge metrics from the boolean flag to the metrics configuration through Config Map
 */
public class BridgeMetricsConversion implements Conversion<KafkaBridge> {
    private static final ThreadLocal<Map<String, String>> TL = new ThreadLocal<>();

    private final Conversion<KafkaBridge> conversion;

    /**
     * Bridge metrics conversion constructor
     */
    public BridgeMetricsConversion() {
        conversion = Conversion.replace("/spec", new BridgeMetricsConversionFunction(KafkaBridgeSpec.class));
    }

    /**
     * Converts the metrics in a JsonNode object
     *
     * @param node The node to convert
     */
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

    /**
     * Converts the metrics in a KafkaBridge object
     *
     * @param instance  The bridge resource to convert
     */
    @Override
    public void convert(KafkaBridge instance) {
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

    private static class BridgeMetricsConversionFunction extends DefaultConversionFunction<KafkaBridgeSpec> {
        private final Class<KafkaBridgeSpec> clazz;

        BridgeMetricsConversionFunction(Class<KafkaBridgeSpec> clazz) {
            this.clazz = clazz;
        }

        String generateName(String suffix) {
            String specName = TL.get().get("name");
            if (specName == null) {
                throw new IllegalStateException("Missing ObjectMeta::name on root resource!");
            }
            return String.format("%s-bridge-jmx-exporter-configuration%s", specName, suffix);
        }

        String newConfigMapKey() {
            return generateName(".yaml");
        }

        String newConfigMapName() {
            return generateName("");
        }

        @Override
        Class<KafkaBridgeSpec> convertedType() {
            return clazz;
        }

        /**
         * Converts the KafkaBridgeSpec object
         *
         * @param holder    The object to convert
         *
         * @return  Converted object
         */
        @Override
        @SuppressWarnings("deprecation")
        public KafkaBridgeSpec apply(KafkaBridgeSpec holder) {
            if (holder == null) {
                return null;
            }

            if (holder.getEnableMetrics() && holder.getMetricsConfig() == null) {
                String key = newConfigMapKey();
                String name = newConfigMapName();

                JmxPrometheusExporterMetrics mc = new JmxPrometheusExporterMetrics();
                ExternalConfigurationReference valueFrom = new ExternalConfigurationReference();
                valueFrom.setConfigMapKeyRef(new ConfigMapKeySelector(key, name, null));
                mc.setValueFrom(valueFrom);
                holder.setMetricsConfig(mc);

                ConfigMap configMap = new ConfigMapBuilder()
                    .withMetadata(new ObjectMetaBuilder().withName(name).build())
                    .withData(Collections.singletonMap(key, defaultMetricsConfiguration()))
                    .build();

                MultipartConversions.get().addLast(new MultipartResource(
                    name,
                    configMap,
                    (c, namespace) -> c.configMaps().inNamespace(namespace).resource(configMap).createOr(Updatable::update)
                ));

                holder.setEnableMetrics(null);
            }

            return holder;
        }
    }

    /**
     * Returns the default metrics configuration String. This is used as a method to avoid Java coping the constant
     * around and Spotbugs complaining.
     *
     * @return  Default metrics configuration
     */
    public static String defaultMetricsConfiguration() {
        return """
                lowercaseOutputName: true
                
                rules:
                  # more specific rules to consumer and producer with topic related information
                  - pattern: kafka.producer<type=(.+), client-id=(.+), topic=(.+)><>([a-z-]+)-total
                    name: strimzi_bridge_kafka_producer_$4_total
                    type: COUNTER
                    labels:
                      type: "$1"
                      clientId: "$2"
                      topic: "$3"
                  - pattern: kafka.producer<type=(.+), client-id=(.+), topic=(.+)><>([a-z-]+)
                    name: strimzi_bridge_kafka_producer_$4
                    type: GAUGE
                    labels:
                      type: "$1"
                      clientId: "$2"
                      topic: "$3"
                  - pattern: kafka.consumer<type=(.+), client-id=(.+), topic=(.+)><>([a-z-]+)-total
                    name: strimzi_bridge_kafka_consumer_$4_total
                    type: COUNTER
                    labels:
                      type: "$1"
                      clientId: "$2"
                      topic: "$3"
                  - pattern: kafka.consumer<type=(.+), client-id=(.+), topic=(.+)><>([a-z-]+)
                    name: strimzi_bridge_kafka_consumer_$4
                    type: GAUGE
                    labels:
                      type: "$1"
                      clientId: "$2"
                      topic: "$3"
                  # more general metrics
                  - pattern: kafka.(\\w+)<type=(.+), client-id=(.+)><>([a-z-]+-total-[a-z-]+) # handles the metrics with total in the middle of the metric name
                    name: strimzi_bridge_kafka_$1_$4
                    type: GAUGE
                    labels:
                      type: "$2"
                      clientId: "$3"
                  - pattern: kafka.(\\w+)<type=(.+), client-id=(.+)><>([a-z-]+)-total
                    name: strimzi_bridge_kafka_$1_$4_total
                    type: COUNTER
                    labels:
                      type: "$2"
                      clientId: "$3"
                  - pattern: kafka.(\\w+)<type=(.+), client-id=(.+)><>([a-z-]+)
                    name: strimzi_bridge_kafka_$1_$4
                    type: GAUGE
                    labels:
                      type: "$2"
                      clientId: "$3"
                  # OAuth Metrics
                  # WARNING: Make sure that the ordering of the attributes is the same as in MBean names
                  - pattern: "strimzi.oauth<type=(.+), context=(.+), kind=(.+), host=\\"(.+)\\", path=\\"(.+)\\", (.+)=(.+), (.+)=(.+), (.+)=(.+)><>(count|totalTimeMs):"
                    name: "strimzi_oauth_$1_$12"
                    type: COUNTER
                    labels:
                      context: "$2"
                      kind: "$3"
                      host: "$4"
                      path: "$5"
                      "$6": "$7"
                      "$8": "$9"
                      "$10": "$11"
                  - pattern: "strimzi.oauth<type=(.+), context=(.+), kind=(.+), host=\\"(.+)\\", path=\\"(.+)\\", (.+)=(.+), (.+)=(.+)><>(count|totalTimeMs):"
                    name: "strimzi_oauth_$1_$10"
                    type: COUNTER
                    labels:
                      context: "$2"
                      kind: "$3"
                      host: "$4"
                      path: "$5"
                      "$6": "$7"
                      "$8": "$9"
                  - pattern: "strimzi.oauth<type=(.+), context=(.+), kind=(.+), host=\\"(.+)\\", path=\\"(.+)\\", (.+)=(.+)><>(count|totalTimeMs):"
                    name: "strimzi_oauth_$1_$8"
                    type: COUNTER
                    labels:
                      context: "$2"
                      kind: "$3"
                      host: "$4"
                      path: "$5"
                      "$6": "$7"
                  - pattern: "strimzi.oauth<type=(.+), context=(.+), kind=(.+), host=\\"(.+)\\", path=\\"(.+)\\", (.+)=(.+), (.+)=(.+), (.+)=(.+)><>(.+):"
                    name: "strimzi_oauth_$1_$12"
                    type: GAUGE
                    labels:
                      context: "$2"
                      kind: "$3"
                      host: "$4"
                      path: "$5"
                      "$6": "$7"
                      "$8": "$9"
                      "$10": "$11"
                  - pattern: "strimzi.oauth<type=(.+), context=(.+), kind=(.+), host=\\"(.+)\\", path=\\"(.+)\\", (.+)=(.+), (.+)=(.+)><>(.+):"
                    name: "strimzi_oauth_$1_$10"
                    type: GAUGE
                    labels:
                      context: "$2"
                      kind: "$3"
                      host: "$4"
                      path: "$5"
                      "$6": "$7"
                      "$8": "$9"
                  - pattern: "strimzi.oauth<type=(.+), context=(.+), kind=(.+), host=\\"(.+)\\", path=\\"(.+)\\", (.+)=(.+)><>(.+):"
                    name: "strimzi_oauth_$1_$8"
                    type: GAUGE
                    labels:
                      context: "$2"
                      kind: "$3"
                      host: "$4"
                      path: "$5"
                      "$6": "$7"
                """;
    }
}
