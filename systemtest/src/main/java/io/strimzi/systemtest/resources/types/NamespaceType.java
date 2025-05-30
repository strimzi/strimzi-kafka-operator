/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.types;

import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceList;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.skodjob.testframe.interfaces.ResourceType;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.NamespaceUtils;

import java.util.function.Consumer;

public class NamespaceType implements ResourceType<Namespace> {

    private final NonNamespaceOperation<Namespace, NamespaceList, Resource<Namespace>> client;

    /**
     * Constructor
     */
    public NamespaceType() {
        this.client = KubeResourceManager.get().kubeClient().getClient().namespaces();
    }

    /**
     * Kind of api resource
     *
     * @return kind name
     */
    @Override
    public String getKind() {
        return "Namespace";
    }

    /**
     * Get specific client for resoruce
     *
     * @return specific client
     */
    @Override
    public NonNamespaceOperation<?, ?, ?> getClient() {
        return client;
    }

    /**
     * Creates specific {@link Namespace} resource
     *
     * @param resource {@link Namespace} resource
     */
    @Override
    public void create(Namespace resource) {
        client.resource(resource).create();
    }

    /**
     * Updates specific {@link Namespace} resource
     *
     * @param resource {@link Namespace} resource that will be updated
     */
    @Override
    public void update(Namespace resource) {
        client.resource(resource).update();
    }

    /**
     * Deletes {@link Namespace} resource from Namespace in current context
     *
     * @param resource {@link Namespace} resource that will be deleted
     */
    @Override
    public void delete(Namespace resource) {
        client.withName(resource.getMetadata().getName()).delete();
    }

    /**
     * Replaces {@link Namespace} resource using {@link Consumer}
     * from which is the current {@link Namespace} resource updated
     *
     * @param resource {@link Namespace} resource that will be replaced
     * @param editor   {@link Consumer} containing updates to the resource
     */
    @Override
    public void replace(Namespace resource, Consumer<Namespace> editor) {
        Namespace toBeUpdated = client.withName(resource.getMetadata().getName()).get();
        editor.accept(toBeUpdated);
        update(toBeUpdated);
    }

    /**
     * Checks if {@link Namespace} exists and is not null.
     *
     * @param resource resource
     * @return result of the readiness check
     */
    @Override
    public boolean isReady(Namespace resource) {
        return resource != null;
    }

    /**
     * Checks if the {@link Namespace} is deleted.
     * If not, it checks if the {@link Namespace} is stuck on finalizers (for KafkaTopics).
     * In case that it's really stuck on KafkaTopic finalizers, it sets the finalizers to null and
     * then returns `false` from this check.
     *
     * @param resource resource
     * @return result of the deletion
     */
    @Override
    public boolean isDeleted(Namespace resource) {
        if (resource == null) {
            return true;
        } else if (NamespaceUtils.isNamespaceDeletionStuckOnFinalizers(resource.getStatus())) {
            String namespaceName = resource.getMetadata().getName();
            KafkaTopicUtils.setFinalizersInAllTopicsToNull(namespaceName);
        }

        return false;
    }
}
