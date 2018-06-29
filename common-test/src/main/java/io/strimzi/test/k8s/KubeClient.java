/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import java.io.File;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

/**
 * Abstraction for a kubernetes client.
 * @param <K> The subtype of KubeClient, for fluency.
 */
public interface KubeClient<K extends KubeClient<K>> {

    static KubeClient<?> findClient(KubeCluster cluster) {
        KubeClient client = null;
        List<KubeClient> kubeClients = Arrays.asList(cluster.defaultClient(), new Kubectl(), new Oc());
        for (KubeClient kc: kubeClients) {
            if (kc.clientAvailable()) {
                client = kc;
                break;
            }
        }
        if (client == null) {
            throw new RuntimeException("No clients found on $PATH, tried " + kubeClients + " $PATH=" + System.getenv("PATH"));
        }
        return client;
    }

    String defaultNamespace();

    /** Deletes the resources by resource name. */
    K deleteByName(String resourceType, String resourceName);

    String namespace(String namespace);

    /** Returns namespace for cluster */
    String namespace();

    boolean clientAvailable();

    /** Creates the resources in the given files. */
    K create(File... files);

    /** Deletes the resources in the given files. */
    K delete(File... files);

    default K create(String... files) {
        return create(asList(files).stream().map(File::new).collect(toList()).toArray(new File[0]));
    }

    default K delete(String... files) {
        return delete(asList(files).stream().map(File::new).collect(toList()).toArray(new File[0]));
    }

    /** Replaces the resources in the given files. */
    K replace(File... files);

    /**
     * Replace with the given YAML.
     * @param yamlContent The replacement YAML.
     * @return This kube client.
     */
    K replaceContent(String yamlContent);

    /** Returns an equivalent client, but logged in as cluster admin. */
    K clientWithAdmin();

    K createContent(String yamlContent);

    K deleteContent(String yamlContent);

    K createNamespace(String name);

    K deleteNamespace(String name);

    /**
     * Execute the given {@code command} in the given {@code pod}.
     * @param pod The pod
     * @param command The command
     * @return The process result.
     */
    ProcessResult exec(String pod, String... command);

    /**
     * Wait for the deployment with the given {@code name} to
     * have replicas==readyReplicas.
     * @param name The deployment name.
     * @return This kube client.
     */
    K waitForDeployment(String name);

    /**
     * Wait for the pod with the given {@code name} to be in the ready state.
     * @param name The pod name.
     * @return This kube client.
     */
    K waitForPod(String name);

    /**
     * Wait for the statefulset with the given {@code name} to have
     * currentReplicas == replicas, and if {@code waitForPods} is true
     * wait for the pods to be in the ready state.
     * * @return This kube client.
     */
    K waitForStatefulSet(String name, int expectPods);

    /**
     * Wait for the resource with the given {@code name} to be created.
     * @param resourceType The resource type.
     * @param resourceName The resource name.
     * @return This kube client.
     */
    K waitForResourceCreation(String resourceType, String resourceName);

    /**
     * Get the content of the given {@code resource} with the given {@code name} as YAML.
     * @param resource The type of resource (e.g. "cm").
     * @param resourceName The name of the resource.
     * @return The resource YAML.
     */
    String get(String resource, String resourceName);

    K waitForResourceDeletion(String resourceType, String resourceName);

    List<String> list(String resourceType);

    String describe(String resourceType, String resourceName);

    String logs(String pod);

    /**
     * @param resourceType The type of resource
     * @param resourceName The name of resource
     * @param sinceSeconds Return logs newer than a relative duration like 5s, 2m, or 3h.
     * @param grepPattern Grep patterns for search
     * @return Grep result as string
     */
    String searchInLog(String resourceType, String resourceName, long sinceSeconds, String... grepPattern);

    String getResourceAsJson(String resourceType, String resourceName);

    K waitForResourceUpdate(String resourceType, String resourceName, Date startTime);

    Date getResourceCreateTimestamp(String pod, String s);

    List<String> listResourcesByLabel(String resourceType, String label);
}
