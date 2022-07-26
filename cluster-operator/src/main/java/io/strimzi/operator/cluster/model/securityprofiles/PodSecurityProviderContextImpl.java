/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.securityprofiles;

import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.plugin.security.profiles.PodSecurityProviderContext;

/**
 * Implements the context for generating the Pod security context
 */
public class PodSecurityProviderContextImpl implements PodSecurityProviderContext {
    private final Storage storage;
    private final PodSecurityContext userSuppliedSecurityContext;

    /**
     * Constructor which can be used when only the user-supplied security context is set, but no storage is used.
     * Storage will be automatically set to null.
     *
     * @param userSuppliedSecurityContext   User-supplied Pod security context
     */
    public PodSecurityProviderContextImpl(PodSecurityContext userSuppliedSecurityContext)   {
        this(null, userSuppliedSecurityContext);
    }

    /**
     * Constructor for setting both user-supplied security context as well as the storage configuration.
     *
     * @param storage                          Storage configuration
     * @param userSuppliedSecurityContext      User-supplied Pod security context
     */
    public PodSecurityProviderContextImpl(Storage storage, PodSecurityContext userSuppliedSecurityContext) {
        this.storage = storage;
        this.userSuppliedSecurityContext = userSuppliedSecurityContext;
    }

    @Override
    public Storage storage() {
        return storage;
    }

    @Override
    public PodSecurityContext userSuppliedSecurityContext() {
        return userSuppliedSecurityContext;
    }
}
