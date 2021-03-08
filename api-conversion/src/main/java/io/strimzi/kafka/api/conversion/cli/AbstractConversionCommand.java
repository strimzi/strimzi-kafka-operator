/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.cli;

import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.kafka.api.conversion.converter.Converter;
import io.strimzi.kafka.api.conversion.converter.KafkaConnectConverter;
import io.strimzi.kafka.api.conversion.converter.KafkaConverter;
import io.strimzi.kafka.api.conversion.converter.KafkaMirrorMaker2Converter;
import io.strimzi.kafka.api.conversion.converter.KafkaTopicConverter;
import io.strimzi.kafka.api.conversion.converter.KafkaUserConverter;
import io.strimzi.kafka.api.conversion.converter.KafkaBridgeConverter;
import io.strimzi.kafka.api.conversion.converter.KafkaConnectS2IConverter;
import io.strimzi.kafka.api.conversion.converter.KafkaConnectorConverter;
import io.strimzi.kafka.api.conversion.converter.KafkaMirrorMakerConverter;
import io.strimzi.kafka.api.conversion.converter.KafkaRebalanceConverter;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("deprecation")
public abstract class AbstractConversionCommand extends AbstractCommand {
    @SuppressWarnings("rawtypes")
    static Map<Object, Converter> converters;

    static {
        converters = new HashMap<>();

        KafkaConverter kc = new KafkaConverter();
        converters.put("Kafka", kc);
        converters.put(Kafka.class, kc);

        KafkaBridgeConverter kbc = new KafkaBridgeConverter();
        converters.put("KafkaBridge", kbc);
        converters.put(KafkaBridge.class, kbc);

        KafkaConnectConverter kcc = new KafkaConnectConverter();
        converters.put("KafkaConnect", kcc);
        converters.put(KafkaConnect.class, kcc);

        KafkaConnectS2IConverter kcs2ic = new KafkaConnectS2IConverter();
        converters.put("KafkaConnectS2I", kcs2ic);
        converters.put(KafkaConnectS2I.class, kcs2ic);

        KafkaMirrorMakerConverter kmmc = new KafkaMirrorMakerConverter();
        converters.put("KafkaMirrorMaker", kmmc);
        converters.put(KafkaMirrorMaker.class, kmmc);

        KafkaMirrorMaker2Converter kmm2c = new KafkaMirrorMaker2Converter();
        converters.put("KafkaMirrorMaker2", kmm2c);
        converters.put(KafkaMirrorMaker2.class, kmm2c);

        KafkaConnectorConverter connectorConverter = new KafkaConnectorConverter();
        converters.put("KafkaConnector", connectorConverter);
        converters.put(KafkaConnector.class, connectorConverter);

        KafkaRebalanceConverter krc = new KafkaRebalanceConverter();
        converters.put("KafkaRebalance", krc);
        converters.put(KafkaRebalance.class, krc);

        KafkaTopicConverter ktc = new KafkaTopicConverter();
        converters.put("KafkaTopic", ktc);
        converters.put(KafkaTopic.class, ktc);

        KafkaUserConverter kuc = new KafkaUserConverter();
        converters.put("KafkaUser", kuc);
        converters.put(KafkaUser.class, kuc);
    }

    @SuppressWarnings("rawtypes")
    protected static Converter getConverter(Object key) {
        Converter converter = converters.get(key);
        if (converter == null) {
            throw new IllegalArgumentException("No converter for key: " + key);
        }
        return converter;
    }
}
