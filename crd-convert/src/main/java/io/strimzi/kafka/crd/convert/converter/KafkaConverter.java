/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.converter;

import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.CruiseControlSpec;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.ZookeeperClusterSpec;
import io.strimzi.api.kafka.model.listener.arraylistener.ArrayOrObjectKafkaListeners;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfiguration;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.template.ExternalServiceTemplate;
import io.strimzi.api.kafka.model.template.ExternalTrafficPolicy;
import io.strimzi.api.kafka.model.template.KafkaClusterTemplate;

import java.util.List;

import static java.util.Arrays.asList;

@SuppressWarnings("unchecked")
public class KafkaConverter extends Converter<Kafka> {

    public static final VersionConversion<Kafka> V1ALPHA1_TO_V1BETA1 = toVersionConversion(ApiVersion.V1ALPHA1, ApiVersion.V1BETA1);

    public static final VersionConversion<Kafka> V1BETA1_TO_V1BETA2 = toVersionConversion(
        ApiVersion.V1BETA1,
        ApiVersion.V1BETA2,
        Conversion.move("/spec/kafka/tolerations", "/spec/kafka/template/pod/tolerations", Conversion.noop()),
        Conversion.move("/spec/kafka/affinity", "/spec/kafka/template/pod/affinity", Conversion.noop()),
        Conversion.move("/spec/zookeeper/tolerations", "/spec/zookeeper/template/pod/tolerations", Conversion.noop()),
        Conversion.move("/spec/zookeeper/affinity", "/spec/zookeeper/template/pod/affinity", Conversion.noop()),
        Conversion.move("/spec/entityOperator/tolerations", "/spec/entityOperator/template/pod/tolerations", Conversion.noop()),
        Conversion.move("/spec/entityOperator/affinity", "/spec/entityOperator/template/pod/affinity", Conversion.noop()),
        Conversion.move("/spec/topicOperator/tlsSidecar", "/spec/entityOperator/tlsSidecar", Conversion.noop()),
        Conversion.move("/spec/topicOperator", "/spec/entityOperator/topicOperator"),
        Conversion.delete("/spec/kafka/tlsSidecar"),
        Conversion.delete("/spec/zookeeper/tlsSidecar"),
        Conversion.replace("/spec/kafka/listeners", new Conversion.DefaultInvertibleFunction<ArrayOrObjectKafkaListeners>() {
                @Override
                Class<ArrayOrObjectKafkaListeners> convertedType() {
                    return ArrayOrObjectKafkaListeners.class;
                }

                @Override
                public ArrayOrObjectKafkaListeners apply(ArrayOrObjectKafkaListeners arrayOrObjectKafkaListeners) {
                    if (arrayOrObjectKafkaListeners != null && arrayOrObjectKafkaListeners.getKafkaListeners() != null) {
                        return new ArrayOrObjectKafkaListeners(arrayOrObjectKafkaListeners.newOrConverted());
                    } else {
                        return arrayOrObjectKafkaListeners;
                    }
                }

                @Override
                public Conversion.InvertibleFunction<ArrayOrObjectKafkaListeners> inverse() {
                    // This is OK because it's OK to have a list listener in v1beta1
                    return this;
                }
            }
        ),
        Conversion.replace("/spec/kafka", new ReplaceExternalServiceTemplate()),
        Conversion.replaceLogging("/spec/kafka/logging", "log4j2.properties"),
        Conversion.replaceLogging("/spec/zookeeper/logging", "log4j2.properties"),
        Conversion.replaceLogging("/spec/entityOperator/topicOperator/logging", "log4j2.properties"),
        Conversion.replaceLogging("/spec/entityOperator/userOperator/logging", "log4j2.properties"),
        Conversion.replaceLogging("/spec/cruiseControl/logging", "log4j2.properties"),
        new MetricsConversion<>("/spec/kafka", KafkaClusterSpec.class),
        new MetricsConversion<>("/spec/zookeeper", ZookeeperClusterSpec.class),
        new MetricsConversion<>("/spec/cruiseControl", CruiseControlSpec.class)
    );

    public KafkaConverter() {
        super(asList(V1ALPHA1_TO_V1BETA1, V1BETA1_TO_V1BETA2));
    }

    @Override
    public Class<Kafka> crClass() {
        return Kafka.class;
    }

    @SuppressWarnings("deprecation")
    static class ReplaceExternalServiceTemplate extends Conversion.DefaultInvertibleFunction<KafkaClusterSpec> {

        private static void apply(KafkaClusterSpec spec, ExternalServiceTemplate template) {
            ArrayOrObjectKafkaListeners listeners = spec.getListeners();
            if (listeners == null) {
                return;
            }
            List<GenericKafkaListener> kafkaListeners = listeners.getGenericKafkaListeners();
            if (kafkaListeners == null || kafkaListeners.isEmpty()) {
                return;
            }

            ExternalTrafficPolicy policy = template.getExternalTrafficPolicy();
            if (policy != null) {
                for (GenericKafkaListener listener : kafkaListeners) {
                    KafkaListenerType type = listener.getType();
                    if (type == KafkaListenerType.LOADBALANCER || type == KafkaListenerType.NODEPORT) {
                        GenericKafkaListenerConfiguration configuration = listener.getConfiguration();
                        if (configuration == null) {
                            configuration = new GenericKafkaListenerConfiguration();
                            listener.setConfiguration(configuration);
                        }
                        configuration.setExternalTrafficPolicy(policy);
                    }
                }
                template.setExternalTrafficPolicy(null);
            }

            List<String> ranges = template.getLoadBalancerSourceRanges();
            if (ranges != null && ranges.size() > 0) {
                for (GenericKafkaListener listener : kafkaListeners) {
                    KafkaListenerType type = listener.getType();
                    if (type == KafkaListenerType.LOADBALANCER) {
                        GenericKafkaListenerConfiguration configuration = listener.getConfiguration();
                        if (configuration == null) {
                            configuration = new GenericKafkaListenerConfiguration();
                            listener.setConfiguration(configuration);
                        }
                        configuration.setLoadBalancerSourceRanges(ranges);
                    }
                }
                template.setLoadBalancerSourceRanges(null);
            }
        }

        @Override
        Class<KafkaClusterSpec> convertedType() {
            return KafkaClusterSpec.class;
        }

        @Override
        public Conversion.InvertibleFunction<KafkaClusterSpec> inverse() {
            return this; // same reason as listeners replace?
        }

        @Override
        public KafkaClusterSpec apply(KafkaClusterSpec spec) {
            if (spec != null) {
                KafkaClusterTemplate kct = spec.getTemplate();
                if (kct != null) {
                    ExternalServiceTemplate bootstrapService = kct.getExternalBootstrapService();
                    ExternalServiceTemplate podService = kct.getPerPodService();
                    if (bootstrapService != null && podService != null) {
                        if (bootstrapService.equals(podService)) {
                            apply(spec, bootstrapService);
                            apply(spec, podService); // called twice, so we nullify both
                        } else {
                            throw new IllegalArgumentException("KafkaClusterSpec's ExternalBootstrapService and PerPodService are not equal!");
                        }
                    } else if (bootstrapService != null) {
                        apply(spec, bootstrapService);
                    } else if (podService != null) {
                        apply(spec, podService);
                    }
                }
            }
            return spec;
        }
    }
}

