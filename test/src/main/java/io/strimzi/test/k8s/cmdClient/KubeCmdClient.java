/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s.cmdClient;

import com.fasterxml.jackson.databind.JsonNode;
import io.strimzi.test.executor.ExecResult;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

/**
 * Abstraction for a kubernetes client.
 * @param <K> The subtype of KubeClient, for fluency.
 */
public interface KubeCmdClient<K extends KubeCmdClient<K>> {

    String defaultNamespace();
    String defaultOlmNamespace();
    String defaultOlmSourceNamespace();


    /** Deletes the resources by resource name. */
    K deleteByName(String resourceType, String resourceName);

    KubeCmdClient<K> namespace(String namespace);

    /** Returns namespace for cluster */
    String namespace();

    /** Creates the resources in the given files. */
    K create(File... files);

    K create(File file);

    /** Creates the resources in the given files. */
    K apply(File... files);

    /** Deletes the resources in the given files. */
    K delete(File... files);

    K createOrReplace(File file);

    default K create(String... files) {
        return create(asList(files).stream().map(File::new).collect(toList()).toArray(new File[0]));
    }

    default K replace(String... files) {
        return replace(asList(files).stream().map(File::new).collect(toList()).toArray(new File[0]));
    }

    default K apply(String... files) {
        return apply(asList(files).stream().map(File::new).collect(toList()).toArray(new File[0]));
    }

    default K delete(String... files) {
        return delete(asList(files).stream().map(File::new).collect(toList()).toArray(new File[0]));
    }

    /** Replaces the resources in the given files. */
    K replace(File... files);

    K applyContent(String yamlContent);

    K createContent(String yamlContent);

    K replaceContent(String yamlContent);

    K deleteContent(String yamlContent);

    K createNamespace(String name);

    K deleteNamespace(String name);

    /**
     * Scale resource using the scale subresource
     *
     * @param kind      Kind of the resource which should be scaled
     * @param name      Name of the resource which should be scaled
     * @param replicas  Number of replicas to which the resource should be scaled
     * @return          This kube client
     */
    K scaleByName(String kind, String name, int replicas);

    /**
     * Execute the given {@code command} in the given {@code pod}.
     * @param pod The pod
     * @param command The command
     * @return The process result.
     */
    ExecResult execInPod(String pod, String... command);

    ExecResult execInCurrentNamespace(String... commands);

    ExecResult execInCurrentNamespace(boolean logToOutput, String... commands);

    /**
     * Execute the given {@code command} in the given {@code container} which is deployed in {@code pod}.
     * @param pod The pod
     * @param container The container
     * @param command The command
     * @return The process result.
     */
    ExecResult execInPodContainer(String pod, String container, String... command);

    ExecResult execInPodContainer(boolean logToOutput, String pod, String container, String... command);

    /**
     * Execute the given {@code command}.
     * @param command The command
     * @return The process result.
     */
    ExecResult exec(String... command);

    /**
     * Execute the given {@code command}. You can specify if potential failure will thrown the exception or not.
     * @param throwError parameter which control thrown exception in case of failure
     * @param command The command
     * @return The process result.
     */
    ExecResult exec(boolean throwError, String... command);

    /**
     * Execute the given {@code command}. You can specify if potential failure will thrown the exception or not.
     * @param throwError parameter which control thrown exception in case of failure
     * @param command The command
     * @param logToOutput determines if we want to print whole output of command
     * @return The process result.
     */
    ExecResult exec(boolean throwError, boolean logToOutput, String... command);

    /**
     * Wait for the resource with the given {@code name} to be reach the state defined by the predicate.
     * @param resource The resource type.
     * @param name The resource name.
     * @param condition Predicate to test if the desired state was achieved
     * @return This kube client.
     */
    K waitFor(String resource, String name, Predicate<JsonNode> condition);

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

    /**
     * Get a list of events in a given namespace
     * @return List of events
     */
    String getEvents();

    K waitForResourceDeletion(String resourceType, String resourceName);

    List<String> list(String resourceType);

    String getResourceAsYaml(String resourceType, String resourceName);

    String getResources(String resourceType);

    String getResourcesAsYaml(String resourceType);

    void createResourceAndApply(String template, Map<String, String> params);

    String describe(String resourceType, String resourceName);

    default String logs(String pod) {
        return logs(pod, null);
    }

    String logs(String pod, String container);

    /**
     * @param resourceType The type of resource
     * @param resourceName The name of resource
     * @param sinceSeconds Return logs newer than a relative duration like 5s, 2m, or 3h.
     * @param grepPattern Grep patterns for search
     * @return Grep result as string
     */
    String searchInLog(String resourceType, String resourceName, long sinceSeconds, String... grepPattern);

    /**
     * @param resourceType The type of resource
     * @param resourceName The name of resource
     * @param resourceContainer The name of resource container
     * @param sinceSeconds Return logs newer than a relative duration like 5s, 2m, or 3h.
     * @param grepPattern Grep patterns for search
     * @return Grep result as string
     */
    String searchInLog(String resourceType, String resourceName, String resourceContainer, long sinceSeconds, String... grepPattern);

    String getResourceAsJson(String resourceType, String resourceName);

    K waitForResourceUpdate(String resourceType, String resourceName, Date startTime);

    Date getResourceCreateTimestamp(String pod, String s);

    List<String> listResourcesByLabel(String resourceType, String label);

    String cmd();

    String getResourceJsonPath(String resourceType, String resourceName, String path);

    boolean getResourceReadiness(String resourceType, String resourceName);

    void patchResource(String resourceType, String resourceName, String patchPath, String value);
}
