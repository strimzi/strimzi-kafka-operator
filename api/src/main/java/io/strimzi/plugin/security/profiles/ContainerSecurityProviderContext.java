/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.security.profiles;

import io.fabric8.kubernetes.api.model.SecurityContext;
import io.strimzi.api.kafka.model.kafka.Storage;

/**
 * Interface which provides the context which can be used to generate the (container) security context
 */
public interface ContainerSecurityProviderContext {
    /**
     * Returns the storage used by the container. If no storage is used, it returns null.
     *
     * @return  Container storage configuration
     */
    Storage storage();

    /**
     * Returns the (container) security context configured by the user in the corresponding `template` section
     *
     * @return Container user-supplied security context
     */
    SecurityContext userSuppliedSecurityContext();
}
