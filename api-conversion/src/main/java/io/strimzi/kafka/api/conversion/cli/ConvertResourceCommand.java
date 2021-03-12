/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.cli;

import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.strimzi.api.kafka.Crds;
import io.strimzi.kafka.api.conversion.converter.ApiConversionFailedException;
import io.strimzi.kafka.api.conversion.converter.MultipartConversions;
import io.strimzi.kafka.api.conversion.converter.MultipartResource;
import picocli.CommandLine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@SuppressWarnings({"rawtypes"})
@CommandLine.Command(name = "convert-resource", aliases = {"cr", "convert-resources"}, description = "Convert Custom Resources directly in Kubernetes")
public class ConvertResourceCommand extends AbstractConversionCommand {
    @CommandLine.Option(names = {"-k", "--kind"}, arity = "0..10", description = "Specifies the kinds of custom resources to be converted, or converts all resources if not specified")
    String[] kinds;

    @CommandLine.ArgGroup
    ConvertResourceCommand.Exclusive exclusive;

    static class Exclusive {
        @CommandLine.Option(names = {"-n", "--namespace"}, description = "Specifies a Kubernetes namespace or OpenShift project, or uses the current namespace if not specified")
        String namespace;

        @CommandLine.Option(names = {"-a", "--all-namespaces"}, description = "Converts resources in all namespaces", defaultValue = "false")
        boolean allNamespaces;
    }

    @CommandLine.Option(names = {"--name"}, description = "Name of the resource which should be converted (can be used onl with --namespace and single --kind options)")
    String name;

    private KubernetesClient client;

    static {
        Crds.registerCustomKinds();
    }

    /**
     * Gets resources of given kind from one or all namespaces
     *
     * @param kind             Array with Kinds which should be converted
     * @param namespace         The namespace in which the resources should be converted
     * @param allNamespaces     Indicates to convert resources in all namespaces
     *
     * @return                  List of found resources for given kind
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected List<CustomResource> get(String kind, String namespace, boolean allNamespaces) {
        MixedOperation<CustomResource, CustomResourceList, ?> op = VERSIONED_OPERATIONS.get(kind).apply(client, OLD_API_VERSIONS.get(kind));

        if (allNamespaces)   {
            return op.inAnyNamespace().list().getItems();
        } else {
            return op.inNamespace(namespace).list().getItems();
        }
    }

    /**
     * Replaces the old resource with the new converted resource
     *
     * @param kind  Kind of the resource
     * @param cr    Converted custom resource
     *
     * @return      Updated custom resource
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected CustomResource replace(String kind, CustomResource cr) {
        MixedOperation<CustomResource, CustomResourceList, ?> op = VERSIONED_OPERATIONS.get(kind).apply(client, TO_API_VERSION.toString());

        return op.inNamespace(cr.getMetadata().getNamespace()).withName(cr.getMetadata().getName()).replace(cr);
    }

    /**
     * Gets a single resource of with a specific kind, namespace and name
     *
     * @param kind      Kind of the resource which should be returned
     * @param name      Name of the resource which should be returned
     * @param namespace Namespace of the resource which should be returned
     *
     * @return          The custom resource obtained from the Kubernetes API
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private CustomResource getNamedResource(String kind, String name, String namespace)    {
        MixedOperation<CustomResource, CustomResourceList, ?> op = VERSIONED_OPERATIONS.get(kind).apply(client, OLD_API_VERSIONS.get(kind));

        return op.inNamespace(namespace).withName(name).get();
    }

    /**
     * Gets resources of given Kinds from one or all namespaces
     *
     * @param kinds             Array with Kinds which should be converted
     * @param namespace         The namespace in which the resources should be converted
     * @param allNamespaces     Indicates to convert resources in all namespaces
     *
     * @return                  List of found resources which should be converted
     */
    private List<CustomResource> getResources(String[] kinds, String namespace, boolean allNamespaces)    {
        List<CustomResource> crs = new ArrayList<>();

        for (String kind : kinds)   {
            crs.addAll(get(kind, namespace, allNamespaces));
        }

        return crs;
    }

    /**
     * Converts the resource to the new APi version
     *
     * @param cr    Custom resource which should be converted
     *
     * @return      The converted custom resource
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected CustomResource convert(CustomResource cr) {
        println("Converting " + cr.getKind() + " resource named " + cr.getMetadata().getName() + " from namespace " + cr.getMetadata().getNamespace());
        try {
            getConverter(cr.getClass()).convertTo(cr, TO_API_VERSION);
            println(cr.getKind() + " resource named " + cr.getMetadata().getName() + " in namespace " + cr.getMetadata().getNamespace() + " has been converted");
            return cr;
        } catch (ApiConversionFailedException e)    {
            println("Failed to convert " + cr.getKind() + " resource named " + cr.getMetadata().getName() + " in namespace " + cr.getMetadata().getNamespace() + ": " + e.getMessage());
            throw new RuntimeException("Failed to convert " + cr.getKind() + " resource named " + cr.getMetadata().getName() + " in namespace " + cr.getMetadata().getNamespace() + ": " + e.getMessage());
        }
    }

    /**
     * When multi-part conversions occurred (conversion which resulted in multiple YAMLs), we have to create them using
     * the Kubernetes client.
     *
     * @param namespace     Namespace where the resource should be created
     */
    private void handleMultipartResources(String namespace) {
        try {
            List<MultipartResource> resources = MultipartConversions.get().getResources();
            for (MultipartResource resource : resources) {
                resource.getK8sConsumer().accept(client, namespace);
            }
        } finally {
            MultipartConversions.remove();
        }
    }

    /**
     * Converts the Custom Resource in Kubernetes
     *
     * @param cr            Custom resource to be converted
     *
     * @throws IOException  Exception if the replacement fails
     */
    private void convertInKube(CustomResource cr) throws IOException {
        CustomResource convertedCr = convert(cr);
        handleMultipartResources(cr.getMetadata().getNamespace());
        replace(cr.getKind(), convertedCr);
    }

    /**
     * Reads the resources from the Kubernetes API and converts them
     */
    @Override
    public void run() {
        try {
            String namespace;
            boolean allNamespaces;
            client = new DefaultKubernetesClient();

            // Handle the --namespace and --all-namespaces options
            if (exclusive == null)  {
                namespace = client.getNamespace();
                allNamespaces = false;
            } else if (exclusive.namespace == null && exclusive.allNamespaces)  {
                namespace = null;
                allNamespaces = true;
            } else if (exclusive.namespace == null)  {
                namespace = client.getNamespace();
                allNamespaces = false;
            } else {
                namespace = exclusive.namespace;
                allNamespaces = exclusive.allNamespaces;
            }

            // Handle the --kind option
            if (kinds == null)  {
                kinds = STRIMZI_KINDS.toArray(String[]::new);
            } else {
                boolean invalidKind = Arrays.stream(kinds).anyMatch(kind -> !STRIMZI_KINDS.contains(kind));

                if (invalidKind) {
                    throw new IllegalArgumentException("Only valid Strimzi custom resource Kinds can be used: " + STRIMZI_KINDS);
                }
            }

            // Handle the --name option
            if (name != null)   {
                if (namespace == null || kinds.length != 1) {
                    throw new IllegalArgumentException("The --name option can be used only with --namespace option and single --kind option");
                }
            }

            // Get the right resources and convert them
            if (name != null)   {
                CustomResource cr = getNamedResource(kinds[0], name, namespace);

                if (cr == null) {
                    throw new IllegalArgumentException("Resource of kind " + kinds[0] + " with name " + name + " in namespace " + namespace + " does not exist!");
                }

                convertInKube(cr);
            } else {
                List<CustomResource> crs = getResources(kinds, namespace, allNamespaces);

                for (CustomResource cr : crs)   {
                    convertInKube(cr);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
