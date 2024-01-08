/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.strimzi.api.kafka.model.common.template.ResourceTemplate;
import io.strimzi.operator.common.model.Labels;

/**
 * Shared methods for working with Service Account
 */
public class ServiceAccountUtils {
    /**
     * Creates a Service Account
     *
     * @param name              Name of the Service Account
     * @param namespace         Namespace of the Service Account
     * @param labels            Labels of the Service Account
     * @param ownerReference    OwnerReference of the Service Account
     * @param template          Service Account template with user's custom configuration
     *
     * @return  New Service Account
     */
    public static ServiceAccount createServiceAccount(
            String name,
            String namespace,
            Labels labels,
            OwnerReference ownerReference,
            ResourceTemplate template
    ) {
        return new ServiceAccountBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withOwnerReferences(ownerReference)
                    .withLabels(labels.withAdditionalLabels(TemplateUtils.labels(template)).toMap())
                    .withAnnotations(TemplateUtils.annotations(template))
                .endMetadata()
                .build();
    }
}
