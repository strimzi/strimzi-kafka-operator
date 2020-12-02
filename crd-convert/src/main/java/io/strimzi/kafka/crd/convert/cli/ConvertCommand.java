/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.cli;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.kafka.crd.convert.converter.Converter;
import io.strimzi.kafka.crd.convert.converter.KafkaBridgeConverter;
import io.strimzi.kafka.crd.convert.converter.KafkaConnectConverter;
import io.strimzi.kafka.crd.convert.converter.KafkaConnectS2IConverter;
import io.strimzi.kafka.crd.convert.converter.KafkaConverter;
import io.strimzi.kafka.crd.convert.converter.KafkaMirrorMaker2Converter;
import io.strimzi.kafka.crd.convert.converter.KafkaMirrorMakerConverter;
import io.strimzi.kafka.crd.convert.converter.KafkaRebalanceConverter;
import io.strimzi.kafka.crd.convert.converter.KafkaTopicConverter;
import io.strimzi.kafka.crd.convert.converter.KafkaUserConverter;
import picocli.CommandLine;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

@SuppressWarnings("deprecation")
@CommandLine.Command(name = "convert", aliases = {"c"}, description = "Convert CRDs")
public class ConvertCommand extends JYCommand {
    @CommandLine.Option(
        names = {"-fv", "--from-version"},
        description = "From K8s ApiVersion - required when used with Kube API",
        completionCandidates = Versions.class
    )
    ApiVersion fromApiVersion;

    @CommandLine.Option(
        names = {"-tv", "--to-version"},
        required = true,
        description = "To K8s ApiVersion",
        completionCandidates = Versions.class
    )
    ApiVersion toApiVersion;

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

        KafkaConnectConverter connectConverter = new KafkaConnectConverter();
        converters.put("KafkaConnect", connectConverter);
        converters.put(KafkaConnect.class, connectConverter);

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
    private static Converter getConverter(Object key) {
        Converter converter = converters.get(key);
        if (converter == null) {
            throw new IllegalArgumentException("No converter for key: " + key);
        }
        return converter;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected MixedOperation<CustomResource, ?, ?> get(BiFunction<KubernetesClient, String, MixedOperation> fn, KubernetesClient client) {
        if (fromApiVersion == null) {
            throw new IllegalArgumentException("Missing from api version argument: -fv=<version>!");
        }
        return fn.apply(client, fromApiVersion.toString());
    }

    @Override
    protected void addArgs(List<String> args) {
        args.add("-fv=" + fromApiVersion.toString());
        args.add("-tv=" + toApiVersion.toString());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected MixedOperation<CustomResource, ?, ?> replace(BiFunction<KubernetesClient, String, MixedOperation> fn, KubernetesClient client) {
        return fn.apply(client, toApiVersion.toString());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected CustomResource run(CustomResource cr) {
        getConverter(cr.getClass()).convertTo(cr, toApiVersion);
        return cr;
    }

    @Override
    protected JsonNode run(JsonNode root) {
        JsonNode kindNode = root.get("kind");
        if (kindNode == null || kindNode.isNull()) {
            throw new IllegalArgumentException("Missing 'kind' node: " + root);
        }
        getConverter(kindNode.asText()).convertTo(root, toApiVersion);
        return root;
    }
}
