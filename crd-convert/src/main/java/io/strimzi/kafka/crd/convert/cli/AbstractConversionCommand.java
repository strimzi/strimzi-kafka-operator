/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.cli;

import io.strimzi.api.annotations.ApiVersion;
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
import io.strimzi.kafka.crd.convert.converter.Converter;
import io.strimzi.kafka.crd.convert.converter.KafkaBridgeConverter;
import io.strimzi.kafka.crd.convert.converter.KafkaConnectConverter;
import io.strimzi.kafka.crd.convert.converter.KafkaConnectS2IConverter;
import io.strimzi.kafka.crd.convert.converter.KafkaConnectorConverter;
import io.strimzi.kafka.crd.convert.converter.KafkaConverter;
import io.strimzi.kafka.crd.convert.converter.KafkaMirrorMaker2Converter;
import io.strimzi.kafka.crd.convert.converter.KafkaMirrorMakerConverter;
import io.strimzi.kafka.crd.convert.converter.KafkaRebalanceConverter;
import io.strimzi.kafka.crd.convert.converter.KafkaTopicConverter;
import io.strimzi.kafka.crd.convert.converter.KafkaUserConverter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("deprecation")
public abstract class AbstractConversionCommand extends AbstractCommand {
    protected static final ApiVersion TO_API_VERSION = ApiVersion.V1BETA2;

    protected static final String STRIMZI_API = "kafka.strimzi.io";
    protected static final Set<String> STRIMZI_KINDS = Set.of(
            "Kafka",
            "KafkaConnect",
            "KafkaConnectS2I",
            "KafkaMirrorMaker",
            "KafkaBridge",
            "KafkaMirrorMaker2",
            "KafkaTopic",
            "KafkaUser",
            "KafkaConnector",
            "KafkaRebalance"
    );

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
