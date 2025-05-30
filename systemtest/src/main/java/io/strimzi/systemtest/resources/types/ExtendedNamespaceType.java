/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.types;

import io.fabric8.kubernetes.api.model.Namespace;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.NamespaceUtils;

/**
 * {@link Namespace} resource type class that is extending the one from Test-Frame - {@link io.skodjob.testframe.resources.NamespaceType}.
 * It changes the {@link #isDeleted(Namespace)} method to also check that the Namespace is stuck (or not) on finalizers.
 */
public class ExtendedNamespaceType extends io.skodjob.testframe.resources.NamespaceType {

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
