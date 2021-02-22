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
import io.strimzi.kafka.crd.convert.utils.ContentType;
import io.strimzi.kafka.crd.convert.utils.IoUtil;
import picocli.CommandLine;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;

@SuppressWarnings({"rawtypes", "deprecation"})
public abstract class ContentCommand extends AbstractCommand {
    @CommandLine.ArgGroup
    Exclusive exclusive;

    @CommandLine.Parameters(arity = "0..", paramLabel = "<Kube API params>", completionCandidates = ContentCommand.Resources.class)
    String[] kubeParams;

    @CommandLine.Option(names = {"-n", "--namespace"}, description = "Kubernetes namespace / OpenShift project", defaultValue = "default")
    String namespace;

    @CommandLine.Option(names = {"-uo", "--openshift"}, description = "Use OpenShift client?")
    boolean openshift;

    private static final Map<String, BiFunction<KubernetesClient, String, MixedOperation>> RESOURCES;

    public static class Resources implements Iterable<String> {
        @Override
        public Iterator<String> iterator() {
            return RESOURCES.keySet().iterator();
        }
    }

    static {
        RESOURCES = new HashMap<>();
        readKubeCustomResourceName(Kafka.class, Crds::kafkaOperation);
        readKubeCustomResourceName(KafkaBridge.class, Crds::kafkaBridgeOperation);
        readKubeCustomResourceName(KafkaConnect.class, Crds::kafkaConnectOperation);
        readKubeCustomResourceName(KafkaConnectS2I.class, Crds::kafkaConnectS2iOperation);
        readKubeCustomResourceName(KafkaMirrorMaker.class, Crds::mirrorMakerOperation);
        readKubeCustomResourceName(KafkaMirrorMaker2.class, Crds::kafkaMirrorMaker2Operation);
        readKubeCustomResourceName(KafkaConnector.class, Crds::kafkaConnectorOperation);
        readKubeCustomResourceName(KafkaRebalance.class, Crds::kafkaRebalanceOperation);
        readKubeCustomResourceName(KafkaTopic.class, Crds::topicOperation);
        readKubeCustomResourceName(KafkaUser.class, Crds::kafkaUserOperation);
    }

    @SuppressWarnings("unchecked")
    static <T extends CustomResource> void readKubeCustomResourceName(Class<T> crClass, BiFunction<KubernetesClient, String, MixedOperation<T, ?, Resource<T>>> op) {
        readKubeCustomResourceNameRaw(crClass, (BiFunction) op);
    }

    private static void readKubeCustomResourceNameRaw(Class<? extends CustomResource> crClass, BiFunction<KubernetesClient, String, MixedOperation> op) {
        Crd crd = crClass.getAnnotation(Crd.class);
        if (crd != null) {
            Crd.Spec spec = crd.spec();
            if (spec != null) {
                Crd.Spec.Names names = spec.names();
                if (names != null) {
                    RESOURCES.put(names.kind(), op);
                    RESOURCES.put(names.kind().toLowerCase(Locale.ROOT), op);
                    RESOURCES.put(names.plural(), op);
                    Arrays.stream(names.shortNames()).forEach(n -> RESOURCES.put(n, op));
                    return;
                }
            }
        }
        throw new IllegalArgumentException("Missing Kube CR info: " + crClass);
    }

    static class Exclusive {
        @CommandLine.Option(names = {"-f", "--file"}, description = "The CR file you want to apply changes")
        File inputFile;

        @CommandLine.Option(names = {"-c", "--content"}, description = "Direct CR content to apply changes")
        String content;
    }

    @CommandLine.Option(names = {"-o", "--output"}, description = "The CR output file after applied changes")
    File outputFile;

    protected String inputExtension;
    protected String outputExtension;

    protected abstract Object run(byte[] data) throws IOException;

    protected abstract CustomResource run(CustomResource cr) throws IOException;

    private boolean useKubeApi() {
        return kubeParams != null && kubeParams.length >= 2 && RESOURCES.containsKey(kubeParams[0]);
    }

    private static String toExtension(File file) {
        String name = file.getName();
        return name.substring(name.lastIndexOf('.') + 1);
    }

    protected abstract MixedOperation<CustomResource, ?, ?> get(BiFunction<KubernetesClient, String, MixedOperation> fn, KubernetesClient client);
    protected abstract void addArgs(List<String> args);
    protected abstract MixedOperation<CustomResource, ?, ?> replace(BiFunction<KubernetesClient, String, MixedOperation> fn, KubernetesClient client);

    @Override
    public void run() {
        try {
            KubernetesClient client = null;
            CustomResource cr = null;
            byte[] data = null;

            if (exclusive != null) {
                if (exclusive.inputFile != null) {
                    inputExtension = toExtension(exclusive.inputFile);
                    data = IoUtil.toBytes(new FileInputStream(exclusive.inputFile));
                } else if (exclusive.content != null && exclusive.content.length() > 0) {
                    data = IoUtil.toBytes(exclusive.content);
                } else {
                    throw new IllegalArgumentException("Missing content!");
                }
            } else if (useKubeApi()) {
                client = openshift ?
                        new DefaultOpenShiftClient().inNamespace(namespace) :
                        new DefaultKubernetesClient().inNamespace(namespace);
                Crds.registerCustomKinds();
                BiFunction<KubernetesClient, String, MixedOperation> function = RESOURCES.get(kubeParams[0]);
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
                String content = data != null ? IoUtil.toString(data) : String.valueOf(cr);
                log.info("Content: " + content);
            }

            if (outputFile != null) {
                outputExtension = toExtension(outputFile);
            }

            if (exclusive != null) {
                Object result = run(data);

                if (outputFile != null && result != null) {
                    Files.copy(
                            new ByteArrayInputStream(IoUtil.toBytes(result.toString())),
                            outputFile.toPath(),
                            StandardCopyOption.REPLACE_EXISTING
                    );
                    result = "Content written to " + outputFile;
                }

                println(String.format("Response [%s]: \n\n" + result, spec.name()));
                println("\n");
            } else {
                cr = run(cr);
                BiFunction<KubernetesClient, String, MixedOperation> function = RESOURCES.get(kubeParams[0]);
                MixedOperation<CustomResource, ?, ?> operation = replace(function, client);
                operation.withName(kubeParams[1]).createOrReplace(cr);
                println(String.format("Custom resource replaced: %s", toArgs()));
            }

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
                } else {
                    byte[] bytes = ContentType.YAML.getMapper().writeValueAsBytes(resource.getResource());
                    if (outputFile != null) {
                        File file = new File(outputFile.getParent(), resource.getName() + "." + ContentType.YAML.getExtension());
                        Files.copy(
                            new ByteArrayInputStream(bytes),
                            file.toPath(),
                            StandardCopyOption.REPLACE_EXISTING
                        );
                        println(String.format("New multipart resource: %s", file));
                    } else {
                        println(String.format("Multipart [%s]: \n\n" + IoUtil.toString(bytes), resource.getName()));
                    }
                }
            }
        } finally {
            MultipartConversions.remove();
        }
    }

    private List<String> toArgs() {
        List<String> args = new ArrayList<>();
        addArgs(args);
        args.add("-n=" + namespace);
        args.addAll(Arrays.asList(kubeParams));
        return args;
    }
}
