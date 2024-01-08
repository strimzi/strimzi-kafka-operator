/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.securityprofiles;

import io.fabric8.kubernetes.api.model.SecurityContext;
import io.strimzi.api.kafka.model.common.template.ContainerTemplate;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.plugin.security.profiles.ContainerSecurityProviderContext;

/**
 * Implements the context for generating the container security context
 */
public class ContainerSecurityProviderContextImpl implements ContainerSecurityProviderContext {
    private final Storage storage;
    private final SecurityContext userSuppliedSecurityContext;

    /**
     * Constructor which can be used when only the user-supplied security context is set, but no storage is used.
     * Storage will be automatically set to null.
     *
     * @param containerTemplate   Container template with user-supplied security context
     */
    public ContainerSecurityProviderContextImpl(ContainerTemplate containerTemplate)   {
        this(null, containerTemplate);
    }

    /**
     * Constructor for setting both user-supplied security context as well as the storage configuration.
     *
     * @param storage               Storage configuration
     * @param containerTemplate     Container template with user-supplied security context
     */
    public ContainerSecurityProviderContextImpl(Storage storage, ContainerTemplate containerTemplate) {
        this.storage = storage;
        this.userSuppliedSecurityContext = containerTemplate != null ? containerTemplate.getSecurityContext() : null;
    }

    @Override
    public Storage storage() {
        return storage;
    }

    @Override
    public SecurityContext userSuppliedSecurityContext() {
        return userSuppliedSecurityContext;
    }
}
