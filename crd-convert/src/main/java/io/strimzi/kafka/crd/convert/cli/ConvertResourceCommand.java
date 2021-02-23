/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.cli;

import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.client.DefaultOpenShiftClient;
import io.strimzi.api.kafka.Crds;
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
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.kafka.crd.convert.converter.MultipartConversions;
import io.strimzi.kafka.crd.convert.converter.MultipartResource;
import picocli.CommandLine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

@SuppressWarnings({"rawtypes", "deprecation"})
@CommandLine.Command(name = "convert-resource", aliases = {"cr"}, description = "Convert Custom Resources directly in Kubernetes")
public abstract class ConvertResourceCommand extends AbstractConversionCommand {
    @CommandLine.Parameters(arity = "0..", paramLabel = "<Kube API params>", completionCandidates = ConvertResourceCommand.Resources.class)
    String[] kubeParams;

    @CommandLine.Option(names = {"-n", "--namespace"}, description = "Kubernetes namespace / OpenShift project", defaultValue = "default")
    String namespace;

    @CommandLine.Option(names = {"-uo", "--openshift"}, description = "Use OpenShift client?")
    boolean openshift;

    private final static Map<String, BiFunction<KubernetesClient, String, MixedOperation>> VERSIONED_OPERATIONS;
    private final static Map<String, Function<KubernetesClient, MixedOperation>> DEFAULT_OPERATIONS;

    public static class Resources implements Iterable<String> {
        @Override
        public Iterator<String> iterator() {
            return VERSIONED_OPERATIONS.keySet().iterator();
        }
    }

    static {
        VERSIONED_OPERATIONS = new HashMap<>();
        versionedOperation(Kafka.class, Crds::kafkaOperation);
        versionedOperation(KafkaBridge.class, Crds::kafkaBridgeOperation);
        versionedOperation(KafkaConnect.class, Crds::kafkaConnectOperation);
        versionedOperation(KafkaConnectS2I.class, Crds::kafkaConnectS2iOperation);
        versionedOperation(KafkaMirrorMaker.class, Crds::mirrorMakerOperation);
        versionedOperation(KafkaMirrorMaker2.class, Crds::kafkaMirrorMaker2Operation);
        versionedOperation(KafkaConnector.class, Crds::kafkaConnectorOperation);
        versionedOperation(KafkaRebalance.class, Crds::kafkaRebalanceOperation);
        versionedOperation(KafkaTopic.class, Crds::topicOperation);
        versionedOperation(KafkaUser.class, Crds::kafkaUserOperation);

        DEFAULT_OPERATIONS = new HashMap<>();
        defaultOperation(Kafka.class, Crds::kafkaOperation);
        defaultOperation(KafkaBridge.class, Crds::kafkaBridgeOperation);
        defaultOperation(KafkaConnect.class, Crds::kafkaConnectOperation);
        defaultOperation(KafkaConnectS2I.class, Crds::kafkaConnectS2iOperation);
        defaultOperation(KafkaMirrorMaker.class, Crds::mirrorMakerOperation);
        defaultOperation(KafkaMirrorMaker2.class, Crds::kafkaMirrorMaker2Operation);
        defaultOperation(KafkaConnector.class, Crds::kafkaConnectorOperation);
        defaultOperation(KafkaRebalance.class, Crds::kafkaRebalanceOperation);
        defaultOperation(KafkaTopic.class, Crds::topicOperation);
        defaultOperation(KafkaUser.class, Crds::kafkaUserOperation);
    }

    @SuppressWarnings("unchecked")
    static <T extends CustomResource> void versionedOperation(Class<T> crClass, BiFunction<KubernetesClient, String, MixedOperation<T, ?, Resource<T>>> op) {
        versionedOperationRaw(crClass, (BiFunction) op);
    }

    private static void versionedOperationRaw(Class<? extends CustomResource> crClass, BiFunction<KubernetesClient, String, MixedOperation> op) {
        Crd crd = crClass.getAnnotation(Crd.class);

        if (crd != null) {
            Crd.Spec.Names names = crd.spec().names();

            VERSIONED_OPERATIONS.put(names.kind(), op);
            VERSIONED_OPERATIONS.put(names.kind().toLowerCase(Locale.ROOT), op);
            VERSIONED_OPERATIONS.put(names.plural(), op);

            Arrays.stream(names.shortNames()).forEach(n -> VERSIONED_OPERATIONS.put(n, op));

            return;
        }

        throw new IllegalArgumentException("Missing Kube CR info: " + crClass);
    }

    @SuppressWarnings("unchecked")
    static <T extends CustomResource> void defaultOperation(Class<T> crClass, Function<KubernetesClient, MixedOperation<T, ?, Resource<T>>> op) {
        defaultOperationRaw(crClass, (Function) op);
    }

    private static void defaultOperationRaw(Class<? extends CustomResource> crClass, Function<KubernetesClient, MixedOperation> op) {
        Crd crd = crClass.getAnnotation(Crd.class);

        if (crd != null) {
            Crd.Spec.Names names = crd.spec().names();

            DEFAULT_OPERATIONS.put(names.kind(), op);
            DEFAULT_OPERATIONS.put(names.kind().toLowerCase(Locale.ROOT), op);
            DEFAULT_OPERATIONS.put(names.plural(), op);

            Arrays.stream(names.shortNames()).forEach(n -> DEFAULT_OPERATIONS.put(n, op));

            return;
        }

        throw new IllegalArgumentException("Missing Kube CR info: " + crClass);
    }


    @SuppressWarnings({"rawtypes", "unchecked"})
    protected CustomResource run(CustomResource cr) {
        getConverter(cr.getClass()).convertTo(cr, TO_API_VERSION);
        return cr;
    }

    private boolean useKubeApi() {
        return kubeParams != null && kubeParams.length >= 2 && VERSIONED_OPERATIONS.containsKey(kubeParams[0]);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected MixedOperation<CustomResource, ?, ?> get(Function<KubernetesClient, MixedOperation> fn, KubernetesClient client) {
        return fn.apply(client);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected MixedOperation<CustomResource, ?, ?> replace(BiFunction<KubernetesClient, String, MixedOperation> fn, KubernetesClient client) {
        return fn.apply(client, TO_API_VERSION.toString());
    }

    @Override
    public void run() {
        try {
            KubernetesClient client;
            CustomResource cr;

            if (useKubeApi()) {
                client = openshift ?
                        new DefaultOpenShiftClient().inNamespace(namespace) :
                        new DefaultKubernetesClient().inNamespace(namespace);
                Crds.registerCustomKinds();
                Function<KubernetesClient, MixedOperation> function = DEFAULT_OPERATIONS.get(kubeParams[0]);
                MixedOperation<CustomResource, ?, ?> operation = get(function, client);
                cr = operation.withName(kubeParams[1]).get();
                if (cr == null) {
                    println(String.format("No such custom resource: %s", toArgs()));
                    return;
                }
            } else {
                println("Invalid usage: missing Kube args or missing content info!");
                return;
            }

            if (debug) {
                log.info("Content: " + cr);
            }

            cr = run(cr);
            BiFunction<KubernetesClient, String, MixedOperation> function = VERSIONED_OPERATIONS.get(kubeParams[0]);
            MixedOperation<CustomResource, ?, ?> operation = replace(function, client);
            operation.withName(kubeParams[1]).createOrReplace(cr);
            println(String.format("Custom resource replaced: %s", toArgs()));

            handleMultipartResources(client);

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void handleMultipartResources(KubernetesClient client) throws IOException {
        try {
            List<MultipartResource> resources = MultipartConversions.get().getResources();
            for (MultipartResource resource : resources) {
                if (client != null) {
                    resource.getK8sConsumer().accept(client, namespace);
                }
            }
        } finally {
            MultipartConversions.remove();
        }
    }

    private List<String> toArgs() {
        List<String> args = new ArrayList<>();
        args.add("-n=" + namespace);
        args.addAll(Arrays.asList(kubeParams));
        return args;
    }
}
