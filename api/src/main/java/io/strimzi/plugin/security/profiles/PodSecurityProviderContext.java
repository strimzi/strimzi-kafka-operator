/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.security.profiles;

import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.strimzi.api.kafka.model.kafka.Storage;

/**
 * Interface which provides the context which can be used to generate the Pod security context
 */
public interface PodSecurityProviderContext {
    /**
     * Returns the storage used by the Pod. If no storage is used, it returns null.
     *
     * @return  Pod storage configuration
     */
    Storage storage();

    /**
     * Returns the Pod security context configured by the user in the corresponding `template` section
     *
     * @return  User-supplied Pod security context
     */
    PodSecurityContext userSuppliedSecurityContext();
}
