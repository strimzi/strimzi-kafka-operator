/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.cli;

import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.kafka.api.conversion.v1.converter.AbstractConverter;
import io.strimzi.kafka.api.conversion.v1.converter.KafkaBridgeConverter;
import io.strimzi.kafka.api.conversion.v1.converter.KafkaConnectConverter;
import io.strimzi.kafka.api.conversion.v1.converter.KafkaConnectorConverter;
import io.strimzi.kafka.api.conversion.v1.converter.KafkaConverter;
import io.strimzi.kafka.api.conversion.v1.converter.KafkaMirrorMaker2Converter;
import io.strimzi.kafka.api.conversion.v1.converter.KafkaNodePoolConverter;
import io.strimzi.kafka.api.conversion.v1.converter.KafkaRebalanceConverter;
import io.strimzi.kafka.api.conversion.v1.converter.KafkaTopicConverter;
import io.strimzi.kafka.api.conversion.v1.converter.KafkaUserConverter;
import io.strimzi.kafka.api.conversion.v1.converter.StrimziPodSetConverter;

import java.util.HashMap;
import java.util.Map;


/**
 * Abstract command class used for both file and in-Kubernetes conversions
 */
public abstract class AbstractConversionCommand extends AbstractCommand {
    @SuppressWarnings({"rawtypes"})
    private static final Map<Object, AbstractConverter> CONVERTERS;
    static {
        CONVERTERS = new HashMap<>();

        KafkaConverter kc = new KafkaConverter();
        CONVERTERS.put("Kafka", kc);
        CONVERTERS.put(Kafka.class, kc);

        KafkaBridgeConverter kbc = new KafkaBridgeConverter();
        CONVERTERS.put("KafkaBridge", kbc);
        CONVERTERS.put(KafkaBridge.class, kbc);

        KafkaConnectConverter kcc = new KafkaConnectConverter();
        CONVERTERS.put("KafkaConnect", kcc);
        CONVERTERS.put(KafkaConnect.class, kcc);

        KafkaMirrorMaker2Converter kmm2c = new KafkaMirrorMaker2Converter();
        CONVERTERS.put("KafkaMirrorMaker2", kmm2c);
        CONVERTERS.put(KafkaMirrorMaker2.class, kmm2c);

        KafkaConnectorConverter connectorConverter = new KafkaConnectorConverter();
        CONVERTERS.put("KafkaConnector", connectorConverter);
        CONVERTERS.put(KafkaConnector.class, connectorConverter);

        KafkaRebalanceConverter krc = new KafkaRebalanceConverter();
        CONVERTERS.put("KafkaRebalance", krc);
        CONVERTERS.put(KafkaRebalance.class, krc);

        KafkaNodePoolConverter knpc = new KafkaNodePoolConverter();
        CONVERTERS.put("KafkaNodePool", knpc);
        CONVERTERS.put(KafkaNodePool.class, knpc);

        KafkaTopicConverter ktc = new KafkaTopicConverter();
        CONVERTERS.put("KafkaTopic", ktc);
        CONVERTERS.put(KafkaTopic.class, ktc);

        KafkaUserConverter kuc = new KafkaUserConverter();
        CONVERTERS.put("KafkaUser", kuc);
        CONVERTERS.put(KafkaUser.class, kuc);

        StrimziPodSetConverter spsc = new StrimziPodSetConverter();
        CONVERTERS.put("StrimziPodSet", spsc);
        CONVERTERS.put(StrimziPodSet.class, spsc);
    }

    @SuppressWarnings({"rawtypes"})
    protected static AbstractConverter getConverter(Object key) {
        AbstractConverter<?> converter = CONVERTERS.get(key);

        if (converter == null) {
            throw new IllegalArgumentException("No converter for key: " + key);
        }

        return converter;
    }
}
