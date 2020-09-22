/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.converter;

import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.listener.arraylistener.ArrayOrObjectKafkaListeners;

import static java.util.Arrays.asList;

public class KafkaConverter extends Converter<Kafka> {

    public static final VersionConversion<Kafka> V1ALPHA1_TO_V1BETA1 = new VersionConversion<>(
            ApiVersion.V1ALPHA1, ApiVersion.V1BETA1);

    public static final VersionConversion<Kafka> V1BETA1_TO_V1BETA2 = new VersionConversion<>(
            ApiVersion.V1BETA1, ApiVersion.V1BETA2,
            asList(Conversion.move("/spec/topicOperator", "/spec/entityOperator/topicOperator"),
            Conversion.move("/spec/kafka/tolerations", "/spec/kafka/template/pod/tolerations", Conversion.noop()),
            Conversion.move("/spec/kafka/affinity", "/spec/kafka/template/pod/affinity", Conversion.noop()),
            Conversion.move("/spec/zookeeper/tolerations", "/spec/zookeeper/template/pod/tolerations", Conversion.noop()),
            Conversion.move("/spec/zookeeper/affinity", "/spec/zookeeper/template/pod/affinity", Conversion.noop()),
            Conversion.delete("/spec/kafka/tlsSidecar"),
            Conversion.delete("/spec/zookeeper/tlsSidecar"),
            Conversion.replace("/spec/kafka/listeners", new Conversion.InvertibleFunction<ArrayOrObjectKafkaListeners>() {
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
            })
    ));

    public KafkaConverter() {
        super(asList(V1ALPHA1_TO_V1BETA1, V1BETA1_TO_V1BETA2));
    }

    @Override
    public Class<Kafka> crClass() {
        return Kafka.class;
    }
}

