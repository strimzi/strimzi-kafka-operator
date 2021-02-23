/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.cli;

import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.openshift.client.DefaultOpenShiftClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.kafka.api.conversion.converter.MultipartConversions;
import io.strimzi.kafka.api.conversion.converter.MultipartResource;
import picocli.CommandLine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

@SuppressWarnings({"rawtypes"})
@CommandLine.Command(name = "convert-resource", aliases = {"cr"}, description = "Convert Custom Resources directly in Kubernetes")
public class ConvertResourceCommand extends AbstractConversionCommand {
    @CommandLine.Option(names = {"-k", "--kind"}, arity = "0..10", description = "Resource Kind which should be converted (if not specified, all Strimzi resources will be converted)")
    String[] kinds;

    @CommandLine.Option(names = {"-n", "--namespace"}, description = "Kubernetes namespace / OpenShift project (if not specified, current namespace will be used)")
    String namespace;

    @CommandLine.Option(names = {"-a", "--all-namespaces"}, description = "Convert resources in all namespaces", defaultValue = "false")
    boolean allNamespaces;

    private final static Map<String, BiFunction<KubernetesClient, String, MixedOperation>> VERSIONED_OPERATIONS = Map.of(
            "Kafka", Crds::kafkaOperation,
            "KafkaConnect", Crds::kafkaConnectOperation,
            "KafkaConnectS2I", Crds::kafkaConnectS2iOperation,
            "KafkaMirrorMaker", Crds::mirrorMakerOperation,
            "KafkaBridge", Crds::kafkaBridgeOperation,
            "KafkaMirrorMaker2", Crds::kafkaMirrorMaker2Operation,
            "KafkaTopic", Crds::kafkaUserOperation,
            "KafkaUser", Crds::topicOperation,
            "KafkaConnector", Crds::kafkaConnectorOperation,
            "KafkaRebalance", Crds::kafkaRebalanceOperation
    );

    private final static Map<String, Function<KubernetesClient, MixedOperation>> DEFAULT_OPERATIONS = Map.of(
            "Kafka", Crds::kafkaOperation,
            "KafkaConnect", Crds::kafkaConnectOperation,
            "KafkaConnectS2I", Crds::kafkaConnectS2iOperation,
            "KafkaMirrorMaker", Crds::mirrorMakerOperation,
            "KafkaBridge", Crds::kafkaBridgeOperation,
            "KafkaMirrorMaker2", Crds::kafkaMirrorMaker2Operation,
            "KafkaTopic", Crds::kafkaUserOperation,
            "KafkaUser", Crds::topicOperation,
            "KafkaConnector", Crds::kafkaConnectorOperation,
            "KafkaRebalance", Crds::kafkaRebalanceOperation
    );

    private KubernetesClient client;

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected CustomResource run(CustomResource cr) {
        getConverter(cr.getClass()).convertTo(cr, TO_API_VERSION);
        return cr;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected List<CustomResource> get(String kind, String namespace, boolean allNamespace) {
        MixedOperation<CustomResource, CustomResourceList, ?> op = DEFAULT_OPERATIONS.get(kind).apply(client);

        if (allNamespace)   {
            return op.inAnyNamespace().list().getItems();
        } else {
            return op.inNamespace(namespace).list().getItems();
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected CustomResource replace(String kind, CustomResource cr) {
        MixedOperation<CustomResource, CustomResourceList, ?> op = VERSIONED_OPERATIONS.get(kind).apply(client, TO_API_VERSION.toString());

        return op.createOrReplace(cr);
    }

    @Override
    public void run() {
        try {
            client = new DefaultOpenShiftClient();
            Crds.registerCustomKinds();

            if (namespace == null && !allNamespaces)  {
                namespace = client.getNamespace();
            }

            if (kinds == null)  {
                kinds = STRIMZI_KINDS.toArray(String[]::new);
            } else {
                boolean invalidKind = Arrays.stream(kinds).anyMatch(kind -> !STRIMZI_KINDS.contains(kind));

                if (invalidKind) {
                    throw new IllegalArgumentException("Only valid Strimzi custom resource Kinds can be used: " + STRIMZI_KINDS);
                }
            }

            for (String kind : kinds)   {
                List<CustomResource> crs = get(kind, namespace, allNamespaces);

                for (CustomResource cr : crs)   {
                    CustomResource convertedCr = run(cr);
                    handleMultipartResources(cr.getMetadata().getNamespace());
                    replace(kind, convertedCr);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void handleMultipartResources(String namespace) throws IOException {
        try {
            List<MultipartResource> resources = MultipartConversions.get().getResources();
            for (MultipartResource resource : resources) {
                resource.getK8sConsumer().accept(client, namespace);
            }
        } finally {
            MultipartConversions.remove();
        }
    }
}
